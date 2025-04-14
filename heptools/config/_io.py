from collections import defaultdict
from pathlib import PurePosixPath
from typing import Any, Callable, Generator, Iterable, TypeVar, overload
from urllib.parse import parse_qs, unquote, urlparse

import fsspec

T = TypeVar("T")


def _unpack(seq: list):
    if len(seq) == 1:
        return seq[0]
    return seq


class FileLoader:
    """
    A module to load and deserialize objects from URL.
    """

    __cache: dict[str, dict[str, Any]] = defaultdict(dict)
    __deserializers: dict[str, Callable[[bytes], Any]] = {}

    @classmethod
    def __load(cls, url: str):
        suffix = PurePosixPath(url).suffixes[0].lstrip(".")
        if not suffix in cls.__deserializers:
            raise NotImplementedError(f"Unsupported file type: {suffix}")

        cache = cls.__cache[suffix]
        if url in cache:
            return cache[url]

        with fsspec.open(url, mode="rb", compression="infer") as f:
            data = f.read()
        data = cls.__deserializers[suffix](data)
        cache[url] = data
        return data

    @classmethod
    def clear_cache(cls):
        """Clear all cache."""
        cls.__cache.clear()

    @classmethod
    @overload
    def register(cls, extensions: str | Iterable[str], deserializer: T) -> T: ...
    @classmethod
    @overload
    def register(cls, extensions: str | Iterable[str]) -> Callable[[T], T]: ...
    @classmethod
    def register(cls, extensions: str | Iterable[str], deserializer=None):
        """
        Register a deserializer for file extensions.
        This method can be used as a decorator.
        The cache for the extensions will be cleared after registration.

        Parameters
        ----------
        extensions : str | Iterable[str]
            File extensions to register.
        deserializer : Callable[[bytes], Any], optional
            Deserializer function. If not provided, will return a decorator.
        """
        if deserializer is None:
            return lambda deserializer: cls.register(extensions, deserializer)
        if isinstance(extensions, str):
            extensions = (extensions,)
        for extension in extensions:
            cls.__deserializers[extension] = deserializer
            cls.__cache.pop(extension, None)
        return deserializer

    @classmethod
    def unregister(cls, extensions: str | Iterable[str]):
        """
        Unregister deserializers for file extensions.
        The cache for the extensions will be cleared after unregistration.

        Parameters
        ----------
        extensions : str | Iterable[str]
            File extensions to unregister.
        """
        if isinstance(extensions, str):
            extensions = (extensions,)
        for extension in extensions:
            cls.__deserializers.pop(extension, None)
            cls.__cache.pop(extension, None)

    @classmethod
    def registered_extensions(cls):
        """Get all registered file extensions."""
        return sorted(cls.__deserializers)

    @classmethod
    def load(cls, url: str, parse_query: bool = True) -> Generator[Any, None, None]:
        """
        Load and deserialize an object from a URL.

        Parameters
        ----------
        url : str
            A URL to an object.
        parse_query : bool, optional
            Whether to parse the query string, by default True
        """
        parsed = urlparse(url)
        path = unquote(parsed.path)
        data = cls.__load(
            parsed._replace(path=path, params="", query="", fragment="").geturl()
        )

        if parsed.fragment:
            for k in parsed.fragment.split("."):
                if isinstance(data, list):
                    k = int(k)
                data = data[k]
        yield data

        if parse_query and parsed.query:
            import json

            query = parse_qs(parsed.query)
            if (q := query.pop("json", ...)) is not ...:
                for v in q:
                    yield json.loads(v)
            if query:
                yield dict(
                    (k, _unpack([*map(json.loads, v)])) for k, v in query.items()
                )


@FileLoader.register("json")
def json_deserializer(data: bytes):
    import json

    return json.loads(data)


@FileLoader.register(("yaml", "yml"))
def yaml_deserializer(data: bytes):
    import yaml

    return yaml.safe_load(data)


@FileLoader.register("pkl")
def pkl_deserializer(data: bytes):
    import pickle

    return pickle.loads(data)


@FileLoader.register("toml")
def toml_deserializer(data: bytes):
    import tomllib

    return tomllib.loads(data.decode("utf-8"))


@FileLoader.register("ini")
def ini_deserializer(data: bytes):
    import configparser

    parser = configparser.ConfigParser()
    parser.read_string(data.decode("utf-8"))
    return {k: dict(v.items()) for k, v in parser.items()}
