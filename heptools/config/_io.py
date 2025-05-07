from __future__ import annotations

import json
import pickle
from collections import defaultdict
from io import BytesIO
from os import fspath, getcwd
from pathlib import PurePosixPath
from types import MethodType
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Iterable,
    Optional,
    TypeVar,
    overload,
    Literal,
)
from urllib.parse import parse_qs, unquote, urlparse

import fsspec
import fsspec.utils

T = TypeVar("T")
HandlerT = TypeVar("HandlerT", bound=Callable)


def _unpack(seq: list):
    if len(seq) == 1:
        return seq[0]
    return seq


def _maybe_json(data: str):
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        return data


def resolve_path(
    base: Optional[str], scheme: Optional[Literal["absolute", "relative"]], *paths: str
):
    base = base or getcwd()
    match scheme:
        case "absolute":
            yield from paths
        case "relative":
            base_parsed = urlparse(base)
            base_path = PurePosixPath(base_parsed.path)
            base_parent = base_path.parent
            for path in paths:
                path_parsed = urlparse(path)
                if path_parsed.path == ".":
                    path_abs = base_path
                else:
                    path_abs = base_parent / path_parsed.path
                yield path_parsed._replace(
                    scheme=base_parsed.scheme,
                    netloc=base_parsed.netloc,
                    path=fspath(path_abs),
                ).geturl()
        case None:
            absolute = []
            relative = []
            order = []
            for path in paths:
                path_parsed = urlparse(path)
                if path_parsed.scheme or PurePosixPath(path).is_absolute():
                    order.append((absolute, len(absolute)))
                    absolute.append(path)
                else:
                    order.append((relative, len(relative)))
                    relative.append(path)
            relative[:] = resolve_path(base, "relative", *relative)
            for col, idx in order:
                yield col[idx]
        case _:
            raise ValueError(f"Unknown path type: {scheme}")


def load_url(
    loader: Callable[[str], Any], url: str, parse_query: bool = True
) -> Generator[Any, None, None]:
    parsed = urlparse(url)
    data = loader(
        parsed._replace(
            path=unquote(parsed.path), params="", query="", fragment=""
        ).geturl()
    )

    if parsed.fragment:
        for k in parsed.fragment.split("."):
            if isinstance(data, list):
                k = int(k)
            data = data[k]
    yield data

    if parse_query and parsed.query:
        query = parse_qs(parsed.query)
        if query:
            yield dict((k, _unpack([*map(_maybe_json, v)])) for k, v in query.items())


class _MaybeClassMethod:
    def __init__(self, func: Callable):
        self.__func = func

    def __get__(self, instance, owner):
        if instance is None:
            return MethodType(self.__func, owner)
        return MethodType(self.__func, instance)


def _maybe_classmethod(method: T) -> T:
    return _MaybeClassMethod(method)


class FileIORegistry(Generic[HandlerT]):
    def __init_subclass__(cls):
        cls.__handler = {}

    def __init__(self):
        self.__handler = {}

    def _get_handler(self, ext: str) -> Callable:
        if handler := self.__handler.get(ext):
            return handler
        if handler := type(self).__handler.get(ext):
            return handler
        raise NotImplementedError(f"Unsupported file type: {ext}")

    @classmethod
    def __normalize_extensions(cls, exts: Iterable[str]):
        for ext in exts:
            yield ext.strip(".").lower()

    @classmethod
    def _get_extensions(cls, url: str) -> tuple[Optional[str], str]:
        suffixes = [*cls.__normalize_extensions(PurePosixPath(url).suffixes)]
        compression = None
        extension = ""
        if not suffixes:
            return compression, extension
        if compression := fsspec.utils.compressions.get(suffixes[-1]):
            suffixes = suffixes[:-1]
        if suffixes:
            extension = suffixes[-1]
        return compression, extension

    @_maybe_classmethod
    def _hook_extension(self, _: str): ...

    @overload
    @classmethod
    def register(cls, handler: HandlerT, *extensions: str) -> None: ...
    @overload
    @classmethod
    def register(cls, *extensions: str) -> Callable[[T], T]: ...
    @_maybe_classmethod
    def register(this, handler=None, *extensions):
        """
        Register a handler for file extensions.
        This method can be used as a decorator.

        Parameters
        ----------
        handler : Callable, optional
            Handler function. If not provided, will return a decorator.
        *extensions : str
            File extensions to register.
        """
        if isinstance(handler, str):

            def wrapper(func):
                this.register(func, handler, *extensions)
                return func

            return wrapper
        for extension in this.__normalize_extensions(extensions):
            this.__handler[extension] = handler
            this._hook_extension(extension)

    @_maybe_classmethod
    def unregister(this, *extensions: str):
        """
        Unregister handlers for file extensions.

        Parameters
        ----------
        *extensions : str
            File extensions to unregister.
        """
        for extension in this.__normalize_extensions(extensions):
            this.__handler.pop(extension, None)
            this._hook_extension(extension)

    @_maybe_classmethod
    def registered(this) -> list[str]:
        """List all registered file extensions."""
        if isinstance(this, type):
            return sorted(this.__handler)
        return sorted(type(this).__handler | this.__handler)


class FileLoader(FileIORegistry[Callable[[BytesIO], Any]]):
    """
    A module to load and deserialize objects from URL.

    Parameters
    ----------
    cache : bool, optional, default=True
        Enable the URL based cache. When an extension is registered or unregistered, the associated cache will be cleared.
    """

    def __init__(self, cache: bool = True):
        super().__init__()
        self.__cache = defaultdict(dict) if cache else None

    @_maybe_classmethod
    def _hook_extension(self, extension: str):
        if not isinstance(self, type) and self.__cache is not None:
            self.__cache.pop(extension, None)

    def load(self, url: str, use_cache: bool = True, use_buffer: bool = True):
        """
        Load objects from URL.

        Parameters
        ----------
        url : str
            URL to an object.
        use_cache: bool, optional, default=True
            If True, use the cache if enabled.
        use_buffer: bool, optional, default=True
            If True, the whole file will be read into memory before deserialization.
        """
        use_cache &= self.__cache is not None
        compression, extension = self._get_extensions(url)
        deserializer = self._get_handler(extension)

        if use_cache:
            cache = self.__cache[extension]
            if url in cache:
                return cache[url]

        with fsspec.open(url, mode="rb", compression=compression) as f:
            if use_buffer:
                data = f.read()
            else:
                data = deserializer(f)
        if use_buffer:
            data = deserializer(BytesIO(data))

        if use_cache:
            cache[url] = data
        return data

    def clear_cache(self):
        """Clear all cache."""
        if self.__cache is not None:
            self.__cache.clear()


@FileLoader.register("json")
def json_deserializer(data: BytesIO):
    return json.load(data)


@FileLoader.register("yaml", "yml")
def yaml_deserializer(data: BytesIO):
    import yaml

    return yaml.safe_load(data)


@FileLoader.register("pkl")
def pkl_deserializer(data: BytesIO):
    return pickle.load(data)


@FileLoader.register("toml")
def toml_deserializer(data: BytesIO):
    import tomllib

    return tomllib.load(data)
