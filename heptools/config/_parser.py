from __future__ import annotations

import operator as op
import re
from collections import defaultdict
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
    include = auto()
    type = auto()
    extend = auto()
    literal = auto()
    code = auto()


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


class PyType:
    @classmethod
    def instance(cls, fullname: str, data):
        # import module and get class/method/function
        import importlib

        clsname = fullname.rsplit("::", 1)
        if len(clsname) == 1:
            modname = "builtins"
            clsname = clsname[0]
        else:
            modname, clsname = clsname
        new = importlib.import_module(modname)
        for name in clsname.split("."):
            new = getattr(new, name)

        # parse args and kwargs
        kwargs = {}
        if isinstance(data, dict):
            kwargs = cls.dict(data)
            args = kwargs.pop(None, [])
        else:
            args = data
        if not isinstance(args, list):
            args = [args]
        return new(*cls.list(args), **kwargs)

    @classmethod
    def dict(cls, data: dict[str, Any], singleton: bool = False):
        parsed = {}
        for k, v in data.items():
            key, flags, v = cls.eval(k, v)
            if (type_flag := flags[FlagKeys.type]).exist:
                v = PyType.instance(type_flag.value, v)
            elif isinstance(v, dict):
                v = cls.dict(v)
            elif isinstance(v, list):
                v = cls.list(v)
            parsed[key] = Extend.merge(flags[FlagKeys.extend], parsed.get(key, ...), v)
        if singleton:
            if (
                len(parsed) == 1
                and None in parsed
                and not flags[FlagKeys.literal].exist
            ):
                return parsed[None]
        return parsed

    @classmethod
    def list(cls, data: list[Any]):
        parsed = []
        for v in data:
            if isinstance(v, dict):
                v = cls.dict(v, singleton=True)
            elif isinstance(v, list):
                v = cls.list(v)
            parsed.append(v)
        return parsed

    @classmethod
    def eval(cls, k: str, v: Any):
        key, flags = Flags.match(k)
        if flags[FlagKeys.code].exist:
            v = eval(v)
        return key, flags, v


class Import:
    ABSOLUTE = "absolute"
    RELATIVE = "relative"

    __current = getcwd()

    @classmethod
    def resolve(cls, *paths: str, flag: str, base: str = None):
        if base is None:
            base = cls.__current
        match flag:
            case cls.ABSOLUTE:
                yield from paths
            case cls.RELATIVE | None:
                base_parsed = urlparse(base)
                parent = PurePosixPath(base_parsed.path).parent
                for path in paths:
                    path_parsed = urlparse(path)
                    yield path_parsed._replace(
                        scheme=base_parsed.scheme,
                        netloc=base_parsed.netloc,
                        path=fspath(parent / path_parsed.path),
                    ).geturl()
            case _:
                raise ValueError(f"Invalid import flag: {flag}")


def _to_stack(*data: dict[str, Any], parent: list[str]):
    stack = []
    for d in data:
        stack.extend((parent + [k], v) for k, v in d.items())
    stack.reverse()
    return stack


def parse_config(
    *path_or_dict: ConfigSource,
    result=None,
    parent=None,
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
        stack = _to_stack(*data, parent=parent)
        while stack:
            k, v = stack.pop()
            k[-1], flags, v = PyType.eval(k[-1], v)
            if (import_flag := flags[FlagKeys.include]).exist:
                if isinstance(v, str):
                    v = [v]
                if isinstance(v, list) and all(isinstance(x, str) for x in v):
                    if k[-1] is None:
                        k = k[:-1]
                    parse_config(
                        *(Import.resolve(*v, flag=import_flag.value, base=path)),
                        result=result,
                        parent=k,
                    )
                    continue
                else:
                    raise ValueError(f"Cannot import from the config files: {v}")
            if k[-1] is None:
                raise ValueError(
                    f"Config key cannot be None or empty: key={k}, flags={flags}"
                )
            if (
                isinstance(v, dict)
                and not flags[FlagKeys.literal].exist
                and not flags[FlagKeys.type].exist
            ):
                stack.extend(_to_stack(v, parent=k))
                continue
            if (type_flag := flags[FlagKeys.type]).exist:
                v = PyType.instance(type_flag.value, v)
            elif isinstance(v, dict):
                v = PyType.dict(v)
            elif isinstance(v, list):
                v = PyType.list(v)
            key = ".".join(k)
            result[key] = Extend.merge(flags[FlagKeys.extend], result.get(key, ...), v)
    return result
