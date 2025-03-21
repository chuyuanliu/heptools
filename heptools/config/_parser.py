from __future__ import annotations

import importlib
import operator as op
import re
from dataclasses import dataclass, field
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


class _FlagParser:
    __match = re.compile(r"(?P<key>[^\>\<]*?)\s*(?P<flags>(\<[^\>\<]*\>\s*)*)\s*")
    __split = re.compile(r"\<(?P<flag>[^\>\<]*)\>")

    def __init__(self, flags: dict[str, Optional[str]] = None):
        self.flags = flags or {}
        self.others = {
            k: v for k, v in self.flags.items() if k not in FlagKeys.__members__
        }

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


class TypeParser:
    def __init__(self, base: str = None):
        self.base = base

    def __call__(self, module: str, key: str, data: Any):
        # import module
        if module is None:
            if not isinstance(data, str):
                raise ValueError(f"Type must be a str, got {data}")
            fullname = data
        else:
            fullname = module
        clsname = fullname.rsplit("::", 1)
        if len(clsname) == 1:
            modname = None
            clsname = clsname[0]
        else:
            modname, clsname = clsname
        if modname is None:
            if self.base is None:
                modname = "builtins"
            else:
                modname = self.base
        elif self.base is not None:
            modname = f"{self.base}.{modname}"

        cls = importlib.import_module(modname)
        for name in clsname.split("."):
            cls = getattr(cls, name)

        if module is None:
            return key, cls

        # parse args and kwargs
        if isinstance(data, dict):
            kwargs = data.copy()
            args = kwargs.pop(None, [])
        else:
            kwargs = {}
            args = data
        if not isinstance(args, list):
            args = [args]
        return key, cls(*args, **kwargs)


class _Parser:
    __include = getcwd()
    __type = TypeParser()
    __extend = MappingProxyType(
        {
            None: op.add,
            "add": op.add,
            "or": op.or_,
            "and": op.and_,
        }
    )

    def __init__(self, flat: bool, base: Optional[str], parser: ConfigParser):
        self.flat = flat
        self.base = base or self.__include
        self.opts = parser

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
            self.setitem(parsed, flags, key, v)
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

    def extend(self, method: str, v1, v2):
        if (func := self.opts.extend_methods.get(method)) is not None:
            ...
        elif (func := self.__extend.get(method)) is not None:
            ...
        else:
            raise ValueError(f"Invalid extend method: {method}")
        return func(v1, v2)

    def eval(self, k: str, v: Any):
        key, flags = _FlagParser.match(k)
        if flags[FlagKeys.code].exist:
            v = eval(v)
        return key, flags, v

    def apply_flags(self, flags: _FlagParser):
        if (type_flag := flags[FlagKeys.type]).exist:
            yield type_flag.value, self.__type
        if not flags.others:
            return
        for flag, custom in self.opts.custom_flags.items():
            if (custom_flag := flags[flag]).exist:
                yield custom_flag.value, custom

    def setitem(self, result: dict[str, Any], flags: _FlagParser, key: str, value: Any):
        if isinstance(value, dict):
            value = self.dict(value)
        elif isinstance(value, list):
            value = self.list(value)
        for flag, method in self.apply_flags(flags):
            key, value = method(flag, key, value)
        if (extend_flag := flags[FlagKeys.extend]).exist and key in result:
            value = self.extend(extend_flag.value, result[key], value)
        result[key] = value

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
            return self.opts._parse(
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


FlagParser = Callable[[Optional[str], str, Any], tuple[str, Any]]
ExtendMethod = Callable[[Any, Any], Any]


@dataclass
class ConfigParser:
    custom_flags: dict[str, FlagParser] = field(default_factory=dict)
    extend_methods: dict[str, ExtendMethod] = field(default_factory=dict)

    def _parse(
        self,
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
            parser = _Parser(flat, path, self)
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
                parser.setitem(result, flags, ".".join(k), v)
        return result

    def __call__(
        self,
        *path_or_dict: ConfigSource,
        flat: bool = False,
        result: Optional[dict[str, Any]] = None,
        parent: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        return self._parse(*path_or_dict, flat=flat, result=result, parent=parent)


class ConfigLoader(ConfigParser):
    def __call__(self, *path_or_dict: ConfigSource, result=None) -> dict[str, Any]:
        return self._parse(*path_or_dict, flat=False, result=result, parent=None)
