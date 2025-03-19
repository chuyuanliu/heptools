from __future__ import annotations

import operator as op
import re
from enum import StrEnum, auto
from functools import cache
from os import PathLike, fspath, getcwd
from pathlib import PurePosixPath
from types import EllipsisType, MappingProxyType
from typing import Any, Callable, Generator, Optional
from urllib.parse import parse_qs, unquote, urlparse
from warnings import warn

from typing_extensions import Self  # DEPRECATE

ConfigSource = str | PathLike | dict[str, Any]


def _unpack(seq: list):
    if len(seq) == 1:
        return seq[0]
    return seq


@cache
def _read_file(url: str) -> str:
    import fsspec

    with fsspec.open(url, mode="rt") as f:
        data = f.read()
    return data


def _parse_file(url: str) -> Generator[dict[str, Any], None, None]:
    parsed = urlparse(url)
    path = unquote(parsed.path)

    if parsed.params:
        warn(f'When parsing "{url}", params will be ignored.')

    data = _read_file(
        parsed._replace(path=path, params="", query="", fragment="").geturl()
    )
    match suffix := PurePosixPath(path).suffix:
        case ".json":
            import json

            data = json.loads(data)
        case ".yaml" | ".yml":
            import yaml

            data = yaml.safe_load(data)
        case ".toml":
            import tomllib

            data = tomllib.loads(data)
        case ".ini":
            import configparser

            parser = configparser.ConfigParser()
            parser.read_string(data)
            data = {k: dict(v.items()) for k, v in parser.items()}
        case _:
            raise NotImplementedError(f"Unsupported file type: {suffix}")

    if parsed.fragment:
        for k in parsed.fragment.split("."):
            data = data[k]
    yield data

    if parsed.query:
        import json

        query = parse_qs(parsed.query)
        if (q := query.pop("json", ...)) is not ...:
            for v in q:
                yield json.loads(v)
        if query:
            yield dict((k, _unpack([*map(json.loads, v)])) for k, v in query.items())


class FlagKeys(StrEnum):
    code = auto()
    include = auto()
    type = auto()
    extend = auto()
    literal = auto()


class Flag:
    def __init__(self, key: EllipsisType | Optional[str]):
        self.flag = key

    @property
    def exist(self):
        return self.flag is not ...

    @property
    def value(self):
        return self.flag


class Flags:
    __match = re.compile(r"(?P<key>[^\>\<]*?)\s*(?P<flags>(\<[^\>\<]*\>\s*)*)\s*")
    __split = re.compile(r"\<(?P<flag>[^\>\<]*)\>")

    def __init__(self, flags: dict[str, Optional[str]] = None):
        self.flags = flags or {}

    def __getitem__(self, flag: str | FlagKeys):
        return Flag(self.flags.get(flag, ...))

    @classmethod
    def match(cls, key: Optional[str]) -> tuple[Optional[str], Self]:
        if key is None:
            return None, cls()
        matched = cls.__match.fullmatch(key)
        if not matched:
            raise ValueError(f"Invalid key: {key}")
        flags = {}
        for flag in cls.__split.finditer(matched["flags"]):
            k = flag["flag"].split("=")
            if len(k) == 1:
                v = None
            elif len(k) == 2:
                v = k[1]
            else:
                raise ValueError(f"Invalid flag: [{flag}] in {key}")
            flags[k[0]] = v
        key = matched["key"]
        if not key or key == "~":
            key = None
        return key, cls(flags)


class ExtendMethods:
    @classmethod
    def register(cls, method: str, func: Callable[[Any, Any], Any]):
        Extend.methods[method] = func

    @property
    def registered(self):
        return MappingProxyType(Extend.methods)


class Extend:
    methods = {
        None: op.add,
        "add": op.add,
        "or": op.or_,
        "and": op.and_,
    }

    @classmethod
    def merge(cls, method: Flag, v1, v2):
        if not method.exist or v1 is ...:
            return v2
        if (func := cls.methods.get(method.value)) is not None:
            return func(v1, v2)
        raise ValueError(f"Invalid extend method: {method.value}")


class Parser:
    __current = getcwd()

    def __init__(self, flat: bool, base: Optional[str] = None):
        self.flat = flat
        self.base = base or self.__current

    def instance(self, flag: Optional[str], data):
        # import module
        import importlib

        if flag is None:
            if not isinstance(data, str):
                raise ValueError(f"Type must be a str, got {data}")
            fullname = data
        else:
            fullname = flag
        clsname = fullname.rsplit("::", 1)
        if len(clsname) == 1:
            modname = "builtins"
            clsname = clsname[0]
        else:
            modname, clsname = clsname
        cls = importlib.import_module(modname)
        for name in clsname.split("."):
            cls = getattr(cls, name)

        if flag is None:
            return cls

        # parse args and kwargs
        kwargs = {}
        if isinstance(data, dict):
            kwargs = self.dict(data)
            args = kwargs.pop(None, [])
        else:
            args = data
        if not isinstance(args, list):
            args = [args]
        return cls(*self.list(args), **kwargs)

    def dict(self, data: dict[str, Any], singleton: bool = False):
        parsed = {}
        for k, v in data.items():
            key, flags, v = self.eval(k, v)
            if (include_flag := flags[FlagKeys.include]).exist:
                if key is None:
                    self.include(v, include_flag, result=parsed)
                    continue
                else:
                    raise ValueError(f"Cannot use include with non-empty key: {key}")
            if (type_flag := flags[FlagKeys.type]).exist:
                v = self.instance(type_flag.value, v)
            elif isinstance(v, dict):
                v = self.dict(v)
            elif isinstance(v, list):
                v = self.list(v)
            parsed[key] = Extend.merge(flags[FlagKeys.extend], parsed.get(key, ...), v)
        if singleton:
            if (
                len(parsed) == 1
                and None in parsed
                and not flags[FlagKeys.literal].exist
            ):
                return parsed[None]
        return parsed

    def list(self, data: list[Any]):
        parsed = []
        for v in data:
            if isinstance(v, dict):
                v = self.dict(v, singleton=True)
            elif isinstance(v, list):
                v = self.list(v)
            parsed.append(v)
        return parsed

    def eval(self, k: str, v: Any):
        key, flags = Flags.match(k)
        if flags[FlagKeys.code].exist:
            v = eval(v)
        return key, flags, v

    def resolve(self, *paths: str, flag: Flag):
        match flag.value:
            case "absolute":
                yield from paths
            case "relative" | None:
                base_parsed = urlparse(self.base)
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
            case _:
                raise ValueError(f"Invalid include flag: {flag}")

    def include(
        self, paths, flag: Flag, result: dict[str, Any] = None, parent: list[str] = None
    ):
        if isinstance(paths, str):
            paths = [paths]
        if isinstance(paths, list) and all(isinstance(path, str) for path in paths):
            return parse_config(
                *self.resolve(*paths, flag=flag),
                flat=self.flat,
                result=result,
                parent=parent,
            )
        raise ValueError(f"Cannot include the configs from: {paths}")


def _to_stack(*data: dict[str, Any], parent: list[str]):
    stack = []
    for d in data:
        stack.extend((parent + [k], v) for k, v in d.items())
    stack.reverse()
    return stack


def parse_config(
    *path_or_dict: ConfigSource,
    flat: bool,
    result: Optional[dict[str, Any]] = None,
    parent: Optional[list[str]] = None,
) -> dict[str, Any]:
    if result is None:
        result = {}
    if parent is None:
        parent = []
    for data in path_or_dict:
        path = None
        if not isinstance(data, dict):
            path = fspath(data)
            data = _parse_file(path)
        else:
            data = (data,)
        parser = Parser(flat, path)
        stack = _to_stack(*data, parent=parent)
        while stack:
            k, v = stack.pop()
            k[-1], flags, v = parser.eval(k[-1], v)
            if (import_flag := flags[FlagKeys.include]).exist:
                if k[-1] is None:
                    k = k[:-1]
                parser.include(v, import_flag, result=result, parent=k)
                continue
            if k[-1] is None:
                raise ValueError(
                    f"Config key cannot be None or empty: key={k}, flags={flags}"
                )
            if (
                flat
                and isinstance(v, dict)
                and not flags[FlagKeys.literal].exist
                and not flags[FlagKeys.type].exist
            ):
                stack.extend(_to_stack(v, parent=k))
                continue
            if (type_flag := flags[FlagKeys.type]).exist:
                v = parser.instance(type_flag.value, v)
            elif isinstance(v, dict):
                v = parser.dict(v)
            elif isinstance(v, list):
                v = parser.list(v)
            key = ".".join(k)
            result[key] = Extend.merge(flags[FlagKeys.extend], result.get(key, ...), v)
    return result


def load_config(*path_or_dict: ConfigSource) -> dict[str, Any]:
    return parse_config(*path_or_dict, flat=False, result=None, parent=None)
