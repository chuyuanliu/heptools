from __future__ import annotations

import copy
import importlib
import inspect
import operator as op
import re
from dataclasses import asdict, dataclass, field
from enum import StrEnum, auto
from functools import cache
from os import PathLike, fspath, getcwd
from pathlib import PurePosixPath
from types import MethodType
from typing import Any, Callable, Optional, ParamSpec, Protocol, TypeVar, cast
from urllib.parse import parse_qs, unquote, urlparse
from warnings import warn

P = ParamSpec("P")
T = TypeVar("T")

ConfigSource = str | PathLike | dict[str, Any]


def _unpack(seq: list):
    if len(seq) == 1:
        return seq[0]
    return seq


@cache  # safe to cache without deepcopy, since parser will always repack dict and list
def _load_file_raw(url: str) -> str:
    import fsspec

    with fsspec.open(url, mode="rb", compression="infer") as f:
        data = cast(bytes, f.read())

    match suffix := PurePosixPath(url).suffixes[0]:
        case ".json":
            import json

            data = json.loads(data)
        case ".yaml" | ".yml":
            import yaml

            data = yaml.safe_load(data)
        case ".pkl":
            import pickle

            data = pickle.loads(data)
        case ".toml":
            import tomllib

            data = tomllib.loads(data.decode("utf-8"))
        case ".ini":
            import configparser

            parser = configparser.ConfigParser()
            parser.read_string(data.decode("utf-8"))
            data = {k: dict(v.items()) for k, v in parser.items()}
        case _:
            raise NotImplementedError(f"Unsupported file type: {suffix}")

    return data


def load_file(url: str, parse_query: bool = True):
    parsed = urlparse(url)
    path = unquote(parsed.path)
    data = _load_file_raw(
        parsed._replace(path=path, params="", query="", fragment="").geturl()
    )

    if parsed.params:
        warn(f'When parsing "{url}", params will be ignored.')

    if parsed.fragment:
        for k in parsed.fragment.split("."):
            try:
                data = data[k]
            except KeyError:
                data = data[int(k)]
    yield data

    if parse_query and parsed.query:
        import json

        query = parse_qs(parsed.query)
        if (q := query.pop("json", ...)) is not ...:
            for v in q:
                yield json.loads(v)
        if query:
            yield dict((k, _unpack([*map(json.loads, v)])) for k, v in query.items())


def clear_cache():
    _load_file_raw.cache_clear()


class FlagKeys(StrEnum):
    code = auto()
    include = auto()
    literal = auto()
    ignore = auto()
    dummy = auto()

    file = auto()
    type = auto()
    var = auto()
    ref = auto()
    copy = auto()
    deepcopy = auto()
    extend = auto()


FlagReserved = frozenset(e.value for e in FlagKeys)


class NoFlag:
    @staticmethod
    def has(_: str):
        return False

    @staticmethod
    def get(_: str):
        return ...

    @staticmethod
    def apply(key: str, value: str):
        return key, value


class Flags:
    def __init__(
        self, flags: Optional[dict[str, Optional[str]]], parsers: dict[str, FlagParser]
    ):
        self.flags = flags or {}
        self.parsers = parsers

    def has(self, flag: str):
        return flag in self.flags

    def get(self, flag: str):
        return self.flags.get(flag, ...)

    def apply(self, *, key: str, value, **kwargs):
        for flag_k, flag_v in self.flags.items():
            if (parser := self.parsers.get(flag_k)) is not None:
                key, value = parser(key=key, value=value, flag=flag_v, **kwargs)
        return key, value

    def __repr__(self):
        return " ".join(f"<{k}={v}>" for k, v in self.flags.items())


class FlagParser(Protocol):
    def __call__(
        self,
        *,
        key: Optional[str],
        value: Optional[Any],
        flag: Optional[str],
        parser: Optional[Parser],
        local: Optional[dict[str, Any]],
    ) -> tuple[str, Any]: ...


class _FlagParser:
    def __init__(self, func: FlagParser):
        self.func = func
        self.keys = frozenset(inspect.signature(func).parameters.keys())

    def __call__(self, *arg, **kwargs):
        return self.func(*arg, **{k: v for k, v in kwargs.items() if k in self.keys})

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return MethodType(self, instance)


def as_flag_parser(func: Callable[P, T]) -> Callable[P, T]:
    return _FlagParser(func)


class TypeParser:
    def __init__(self, base: str = None):
        self.base = base

    @as_flag_parser
    def __call__(self, flag: Optional[str], key: str, value: Any):
        # import module
        if flag is None:
            if not isinstance(value, str):
                raise ValueError(
                    f'Type name must be a string when parsing "{key} <{FlagKeys.type}>: {value}"'
                )
            fullname = value
        else:
            fullname = flag
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
        clsname = clsname.split(".")
        if any(clsname):
            for name in clsname:
                cls = getattr(cls, name)

        if flag is None:
            return key, cls

        # parse args and kwargs
        if isinstance(value, dict):
            kwargs = value.copy()
            args = kwargs.pop(None, [])
        else:
            kwargs = {}
            args = value
        if not isinstance(args, list):
            args = [args]
        return key, cls(*args, **kwargs)


ExtendMethod = Callable[[Any, Any], Any]


class ExtendRecursive:
    def __init__(self, op: ExtendMethod):
        self.op = op

    def __call__(self, v1, v2):
        if isinstance(v1, dict) and isinstance(v2, dict):
            new = v1.copy()
            for k, v in v2.items():
                if k in v1:
                    new[k] = self(v1[k], v)
                else:
                    new[k] = v
        else:
            return self.op(v1, v2)
        return new


class ExtendParser:
    methods = {
        None: ExtendRecursive(op.add),
        "add": op.add,
        "or": op.or_,
        "and": op.and_,
        "recursive": ExtendRecursive(op.add),
    }

    def __init__(self, methods: dict[str, ExtendMethod]):
        if methods:
            self.methods = methods | self.methods

    @as_flag_parser
    def __call__(self, local: dict[str, Any], flag: Optional[str], key: str, value):
        if key not in local:
            return key, value
        try:
            return key, self.methods[flag](local[key], value)
        except KeyError:
            raise ValueError(
                f'Unknown method when parsing "<{FlagKeys.extend}={flag}>"'
            )


class VariableParser:
    def __init__(self):
        self.local = {}

    @staticmethod
    def _get_name(*args):
        for arg in args:
            if isinstance(arg, str):
                return arg
        raise ValueError

    @as_flag_parser
    def var(self, flag: Optional[str], key: str, value):
        try:
            self.local[self._get_name(flag, key)] = value
        except ValueError:
            raise ValueError(
                f'Variable name cannot be None when parsing "<{FlagKeys.var}>"'
            )
        return key, value

    def _get(self, flag: Optional[str], key: str, value, op: str):
        try:
            name = self._get_name(flag, value, key)
            return self.local[name]
        except KeyError:
            raise KeyError(
                f'Variable "{name}" does not exist when parsing "{key} <{op}={flag}>: {value}"'
            )
        except ValueError:
            raise ValueError(f'Variable name cannot be None when parsing "<{op}>"')

    @as_flag_parser
    def ref(self, flag: Optional[str], key: str, value):
        return key, self._get(flag, key, value, FlagKeys.ref)

    @as_flag_parser
    def copy(self, flag: Optional[str], key: str, value):
        return key, copy.copy(self._get(flag, key, value, FlagKeys.copy))

    @as_flag_parser
    def deepcopy(self, flag: Optional[str], key: str, value):
        return key, copy.deepcopy(self._get(flag, key, value, FlagKeys.deepcopy))


class FileParser:
    @as_flag_parser
    def __call__(self, parser: Parser, flag: Optional[str], key: str, value):
        return key, copy.deepcopy(
            next(load_file(next(parser.resolve(value, flag=flag)), parse_query=False))
        )


class Parser:
    __cwd = getcwd()

    def __init__(self, flat: bool, base: Optional[str], custom: _ParserInitializer):
        self.flat = flat
        self.base = base or self.__cwd
        self.custom = custom

    def dict(self, data: dict[str, Any], singleton: bool = False):
        parsed = {}
        for k, v in data.items():
            key, flags, v = self.eval(k, v)
            if (include_flag := flags.get(FlagKeys.include)) is not ...:
                if key is None:
                    self.include(v, include_flag, result=parsed)
                    continue
                else:
                    raise ValueError(f"Cannot use include with non-empty key: {key}")
            self.setitem(parsed, flags, key, v)
        if (
            singleton
            and len(parsed) == 1
            and None in parsed
            and not flags.has(FlagKeys.literal)
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
        key, flags = self.custom.match(k)
        if flags.has(FlagKeys.code):
            v = eval(v, None, self.custom.locals)
        return key, flags, v

    def setitem(self, local: dict[str, Any], flags: Flags, key: str, value: Any):
        if isinstance(value, dict):
            value = self.dict(value)
        elif isinstance(value, list):
            value = self.list(value)
        key, value = flags.apply(parser=self, local=local, key=key, value=value)
        if not flags.has(FlagKeys.ignore):
            local[key] = value

    def resolve(self, *paths: str, flag: str):
        match flag:
            case "absolute":
                yield from paths
            case "relative":
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
                relative[:] = self.resolve(*relative, flag="relative")
                for col, idx in order:
                    yield col[idx]
            case _:
                raise ValueError(f"Invalid include flag: {flag}")

    def include(
        self, paths, flag: str, result: dict[str, Any] = None, parent: list[str] = None
    ):
        if isinstance(paths, str):
            paths = [paths]
        if isinstance(paths, list) and all(isinstance(path, str) for path in paths):
            return self.custom.parse(
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


@dataclass
class _ParserCustomization:
    custom_flags: dict[str, FlagParser] = field(default_factory=dict)
    extend_methods: dict[str, ExtendMethod] = field(default_factory=dict)


@dataclass
class _ParserInitializer(_ParserCustomization):
    _match = re.compile(r"(?P<key>[^\>\<]*?)\s*(?P<flags>(\<[^\>\<]*\>\s*)*)\s*")
    _split = re.compile(r"\<(?P<flag>[^\>\<]*)\>")

    type = TypeParser()
    file = FileParser()

    def __post_init__(self):
        self.vars = VariableParser()
        if reserved := FlagReserved.intersection(self.custom_flags):
            raise RuntimeError(
                f"The following reserved flags are overridden: {', '.join(reserved)}"
            )
        self.parsers = self.custom_flags | {
            FlagKeys.file: self.file,
            FlagKeys.type: self.type,
            FlagKeys.var: self.vars.var,
            FlagKeys.ref: self.vars.ref,
            FlagKeys.copy: self.vars.copy,
            FlagKeys.deepcopy: self.vars.deepcopy,
            FlagKeys.extend: ExtendParser(self.extend_methods),
        }

    @classmethod
    def new(cls, other: _ParserCustomization):
        return cls(**asdict(other))

    @property
    def locals(self):
        return self.vars.local

    def parse(
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
                data = load_file(path)
            else:
                data = (data,)
            parser = Parser(flat, path, self)
            stack = _to_stack(*data, parent=parent)
            while stack:
                k, v = stack.pop()
                k[-1], flags, v = parser.eval(k[-1], v)
                if (import_flag := flags.get(FlagKeys.include)) is not ...:
                    if k[-1] is None:
                        k = k[:-1]
                    parser.include(v, import_flag, result=result, parent=k)
                    continue
                if k[-1] is None:
                    if flags.has(FlagKeys.ignore):
                        k = k[:-1]
                    else:
                        raise ValueError(
                            f"Config key cannot be None or empty: key={k}, flags={flags}"
                        )
                if (
                    flat
                    and isinstance(v, dict)
                    and not flags.has(FlagKeys.literal)
                    and not flags.has(FlagKeys.ignore)
                    and not flags.has(FlagKeys.type)
                ):
                    stack.extend(_to_stack(v, parent=k))
                else:
                    parser.setitem(result, flags, ".".join(k), v)
        return result

    def match(self, key: Optional[str]) -> tuple[Optional[str], Flags]:
        if key is None:
            return None, NoFlag
        matched = self._match.fullmatch(key)
        if not matched:
            raise ValueError(f"Invalid key format: {key}")
        flags = {}
        for flag in self._split.finditer(matched["flags"]):
            k = flag["flag"].split("=")
            if len(k) == 1:
                v = None
            elif len(k) == 2:
                v = k[1]
            else:
                raise ValueError(f"Invalid flag format: <{flag}> in {key}")
            flags[k[0]] = v
        key = matched["key"]
        if not key or key == "~":
            key = None
        return key, Flags(flags, self.parsers)


class GlobalConfigParser(_ParserCustomization):
    def __call__(
        self, *path_or_dict: ConfigSource, result: Optional[dict[str, Any]]
    ) -> dict[str, Any]:
        return _ParserInitializer.new(self).parse(
            *path_or_dict, flat=True, result=result, parent=None
        )


class ConfigLoader(_ParserCustomization):
    def __call__(
        self, *path_or_dict: ConfigSource, result: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        return _ParserInitializer.new(self).parse(
            *path_or_dict, flat=False, result=result, parent=None
        )

    @staticmethod
    def clear_cache():
        clear_cache()
