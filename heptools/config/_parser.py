from __future__ import annotations

import copy
import importlib
import inspect
import operator as op
import re
from dataclasses import dataclass, field, fields
from enum import StrEnum, auto
from os import PathLike, fspath, getcwd
from pathlib import PurePosixPath
from types import MappingProxyType, MethodType
from typing import Any, Callable, Optional, ParamSpec, Protocol, TypeVar
from urllib.parse import urlparse

from ._io import FileLoader
from ._utils import NestedTree

P = ParamSpec("P")
T = TypeVar("T")

ConfigSource = str | PathLike | dict[str, Any]
"""
str, ~os.PathLike, dict: A path to the config file or a nested dict.
"""


class FlagKeys(StrEnum):
    code = auto()
    include = auto()
    discard = auto()
    dummy = auto()

    file = auto()
    type = auto()
    attr = auto()
    var = auto()
    ref = auto()
    copy = auto()
    deepcopy = auto()
    extend = auto()


FlagReserved = frozenset(e.value for e in FlagKeys)


class _NoFlag:
    @staticmethod
    def has(_: str):
        return False

    @staticmethod
    def get(_: str):
        return ...

    @staticmethod
    def apply(*, key: str, value: str, **_):
        return key, value


class _Flags:
    _globals = frozenset((FlagKeys.code, FlagKeys.include, FlagKeys.discard))

    def __init__(
        self, flags: list[tuple[str, str]], parsers: dict[str, Optional[FlagParser]]
    ):
        self.parsers = parsers
        self.flags = list[tuple[str, str]]()
        self.parsed = dict[str, str]()
        for k, v in flags:
            if k in self._globals:
                self.parsed[k] = v
            else:
                self.flags.append((k, v))

    def has(self, flag: str):
        return flag in self.parsed

    def get(self, flag: str):
        return self.parsed.get(flag, ...)

    def apply(self, *, key: str, value, **kwargs):
        for flag_k, flag_v in self.flags:
            if (parser := self.parsers.get(flag_k)) is not None:
                key, value = parser(
                    key=key,
                    value=value,
                    flag=flag_v,
                    flags=MappingProxyType(self.parsed),
                    **kwargs,
                )
            self.parsed[flag_k] = flag_v
        return key, value

    def __repr__(self):
        return " ".join(f"<{k}={v}>" for k, v in self.flags.items())


class FlagParser(Protocol):
    """
    Flag parser protocol
    """

    def __call__(
        self,
        *,
        key: Optional[str],
        value: Optional[Any],
        flag: Optional[str],
        flags: Optional[dict[str, Optional[str]]],
        local: Optional[dict[str, Any]],
        parser: Optional[Parser],
    ) -> tuple[str, Any]:
        """
        Parameters
        ----------
        key: str, optional
            The key of the current item.
        value: Any, optional
            The value of the current item.
        flag: str, optional
            The value of the current flag.
        flags: dict[str, Optional[str]], optional
            All flags before the current flag.
        local: dict[str, Any], optional
            The parsed result.
        parser: Parser, optional
            The parser instance.

        Returns
        -------
        tuple[str, Any]
            The key and value after parsing.
        """
        ...


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
    """
    A decorator to make a function with omitted keyword arguments compatible with :class:`FlagParser` protocol.
    """

    return _FlagParser(func)


class TypeParser:  # flag: <type>
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


class AttrParser:  # flag: <attr>
    @as_flag_parser
    def __call__(self, flag: Optional[str], key: str, value):
        for attr in flag.split("."):
            value = getattr(value, attr)
        return key, value


ExtendMethod = Callable[[Any, Any], Any]
"""
~typing.Callable[[Any, Any], Any]: A method to merge two values into one.
"""


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


class ExtendParser:  # flag: <extend>
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


class VariableParser:  # flag: <var> <ref> <copy> <deepcopy>
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


class FileParser:  # flag: <file>
    @as_flag_parser
    def __call__(self, parser: Parser, flag: Optional[str], key: str, value):
        return key, copy.deepcopy(
            next(
                FileLoader.load(
                    next(parser.resolve(value, flag=flag)), parse_query=False
                )
            )
        )


class Parser:
    __cwd = getcwd()

    def __init__(self, base: Optional[str], custom: _ParserInitializer):
        self.base = base or self.__cwd
        self.custom = custom

    def dict(self, data: dict[str, Any], singleton: bool = False, result: dict = None):
        if result is None:
            result = {}
        for k, v in data.items():
            key, flags, v = self.eval(k, v)
            if (include_flag := flags.get(FlagKeys.include)) is not ...:
                if key is None:
                    self.include(v, include_flag, result=result)
                    continue
                else:
                    raise ValueError(f"Cannot use include with non-empty key: {key}")
            self.setitem(result, flags, key, v)
        if singleton and len(result) == 1 and None in result:
            return result[None]
        return result

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

    def setitem(self, local: dict[str, Any], flags: _Flags, key: str, value: Any):
        if self.custom.expand and key is not None:
            keys = key.split(".")
            local = NestedTree.init(local, keys[:-1])
            key = keys[-1]
        if isinstance(value, dict):
            value = self.dict(value)
        elif isinstance(value, list):
            value = self.list(value)
        key, value = flags.apply(parser=self, local=local, key=key, value=value)
        if not flags.has(FlagKeys.discard):
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

    def include(self, paths, flag: str, result: dict[str, Any] = None):
        if isinstance(paths, str):
            paths = [paths]
        if isinstance(paths, list) and all(isinstance(path, str) for path in paths):
            return self.custom.parse(*self.resolve(*paths, flag=flag), result=result)
        raise ValueError(f"Cannot include the configs from: {paths}")


@dataclass
class _ParserCustomization:
    expand: bool = False
    custom_flags: dict[str, Optional[FlagParser]] = field(default_factory=dict)
    extend_methods: dict[str, ExtendMethod] = field(default_factory=dict)


@dataclass
class _ParserInitializer(_ParserCustomization):
    _match = re.compile(r"(?P<key>.*?)\s*(?P<flags>(\<[^\>\<]*\>\s*)*)\s*")
    _split = re.compile(r"\<(?P<flag>[^\>\<]*)\>")

    type = TypeParser()
    attr = AttrParser()
    file = FileParser()

    def __post_init__(self):
        self.vars = VariableParser()
        self.parsers = self.custom_flags | {
            FlagKeys.code: None,
            FlagKeys.include: None,
            FlagKeys.discard: None,
            FlagKeys.dummy: None,
            FlagKeys.file: self.file,
            FlagKeys.type: self.type,
            FlagKeys.attr: self.attr,
            FlagKeys.var: self.vars.var,
            FlagKeys.ref: self.vars.ref,
            FlagKeys.copy: self.vars.copy,
            FlagKeys.deepcopy: self.vars.deepcopy,
            FlagKeys.extend: ExtendParser(self.extend_methods),
        }

    @classmethod
    def new(cls, other: _ParserCustomization):
        return cls(**{k.name: getattr(other, k.name) for k in fields(other)})

    @property
    def locals(self):
        return self.vars.local

    def parse(
        self,
        *path_or_dict: ConfigSource,
        result: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        if result is None:
            result = {}
        for configs in path_or_dict:
            path = None
            if not isinstance(configs, dict):
                path = fspath(configs)
                configs = FileLoader.load(path)
            else:
                configs = (configs,)
            parser = Parser(path, self)
            for config in configs:
                parser.dict(config, result=result)
        return result

    def match(self, key: Optional[str]) -> tuple[Optional[str], _Flags]:
        if key is None:
            return None, _NoFlag
        matched = self._match.fullmatch(key)
        if not matched:
            return key, _NoFlag
        flags = []
        for flag in self._split.finditer(matched["flags"]):
            k = flag["flag"].split("=")
            if len(k) == 1:
                v = None
            elif len(k) == 2:
                v = k[1]
            else:
                raise ValueError(f"Invalid flag format: <{flag}> in {key}")
            flags.append((k[0], v))
        key = matched["key"]
        if not key or key == "~":
            key = None
        return key, _Flags(flags, self.parsers)


class ConfigParser(_ParserCustomization):
    """
    A customizable config parser.

    Parameters
    ----------
    expand : bool, optional, default=False
        Expand dot-separated keys to nested dicts.
    custom_flags : dict[str, Optional[FlagParser]], optional
        Customized flags
    extend_methods : dict[str, ExtendMethod], optional
        Customized <extend> methods.

    """

    def __call__(
        self, *path_or_dict: ConfigSource, result: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        """
        Load configs from multiple sources.

        Parameters
        ----------
        *path_or_dict : ConfigSource
            Paths to config files or deserialized configs.
        result : dict[str, Any], optional
            If provided, the configs will be loaded into this dict.

        Returns
        -------
        dict[str, Any]
            The loaded configs.
        """
        return _ParserInitializer.new(self).parse(*path_or_dict, result=result)
