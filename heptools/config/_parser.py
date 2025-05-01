from __future__ import annotations

import copy
import importlib
import inspect
import operator as op
import re
from dataclasses import dataclass, field, fields
from enum import StrEnum, auto
from functools import partial
from inspect import _ParameterKind as ParKind
from os import PathLike, fspath, getcwd
from types import MappingProxyType, MethodType
from typing import Any, Callable, Optional, ParamSpec, Protocol, TypeVar

from ._io import FileLoader, load_url, resolve_path
from ._utils import SimpleTree

P = ParamSpec("P")
T = TypeVar("T")

ConfigSource = str | PathLike | dict[str, Any]
"""
str, ~os.PathLike, dict: A path to the config file or a nested dict.
"""


class _ReservedTag(StrEnum):
    def _generate_next_value_(name, *_):
        return name.lower().replace("_", "-")

    code = auto()
    include = auto()
    literal = auto()
    discard = auto()
    dummy = auto()

    file = auto()
    file_cache = auto()
    type = auto()
    attr = auto()
    var = auto()
    ref = auto()
    copy = auto()
    deepcopy = auto()
    extend = auto()


class _NoTag:
    @staticmethod
    def has(_: str):
        return False

    @staticmethod
    def get(_: str):
        return ...

    @staticmethod
    def apply(*, key: str, value: str, **_):
        return key, value


class _MatchedTags:
    special = frozenset(
        (
            _ReservedTag.code,
            _ReservedTag.include,
            _ReservedTag.literal,
            _ReservedTag.discard,
        )
    )

    def __init__(
        self, tags: list[tuple[str, str]], parsers: dict[str, Optional[TagParser]]
    ):
        self.parsers = parsers
        self.tags = list[tuple[str, str]]()
        self.parsed = dict[str, str]()
        for k, v in tags:
            if k in self.special:
                self.parsed[k] = v
            else:
                self.tags.append((k, v))

    def has(self, tag: str):
        return tag in self.parsed

    def get(self, tag: str):
        return self.parsed.get(tag, ...)

    def apply(self, *, key: str, value, **kwargs):
        for tag_k, tag_v in self.tags:
            if (parser := self.parsers.get(tag_k)) is not None:
                key, value = parser(
                    key=key,
                    value=value,
                    tag=tag_v,
                    tags=MappingProxyType(self.parsed),
                    **kwargs,
                )
            self.parsed[tag_k] = tag_v
        return key, value

    def __repr__(self):
        return " ".join(f"<{k}={v}>" for k, v in self.tags)


class TagParser(Protocol):
    """
    Tag parser protocol
    """

    def __call__(
        self,
        *,
        key: Optional[str],
        value: Optional[Any],
        tag: Optional[str],
        tags: Optional[dict[str, Optional[str]]],
        local: Optional[dict[str, Any]],
        path: Optional[str],
    ) -> tuple[str, Any]:
        """
        Parameters
        ----------
        key: str, optional
            The current key.
        value: Any, optional
            The current value .
        tag: str, optional
            The value of the current tag.
        tags: dict[str, Optional[str]], optional
            All parsed tags of the current key.
        local: dict[str, Any], optional
            All parsed items in the current dictionary.
        path: str, optional
            The path to the current config file.

        Returns
        -------
        tuple[str, Any]
            The key and value after parsing.
        """
        ...


@dataclass
class _TagParserWrapper:
    func: TagParser
    keys: frozenset[str]

    def __call__(self, *arg, **kwargs):
        return self.func(*arg, **{k: v for k, v in kwargs.items() if k in self.keys})

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return MethodType(self, instance)


def _tag_parser(func: Callable[P, T]) -> Callable[P, T]:
    kwargs = set()
    sig = inspect.signature(func)
    for k, v in sig.parameters.items():
        match v.kind:
            case ParKind.POSITIONAL_OR_KEYWORD | ParKind.KEYWORD_ONLY:
                kwargs.add(k)
            case ParKind.VAR_KEYWORD:
                return func
    return _TagParserWrapper(func=func, keys=frozenset(kwargs))


class TypeParser:  # tag: <type>
    def __init__(self, base: str = None):
        self.base = base

    @_tag_parser
    def __call__(self, tag: Optional[str], key: str, value: Any):
        # import module
        if tag is None:
            if not isinstance(value, str):
                raise ValueError(
                    f'When parsing "{key} <{_ReservedTag.type}>: {value}":\n  type name must be a string.'
                )
            fullname = value
        else:
            fullname = tag
        clsname = fullname.split("::", 1)
        match len(clsname):
            case 1:
                modname, clsname = None, clsname[0]
            case 2:
                modname, clsname = clsname
            case _:
                raise ValueError(f'Invalid import format "{fullname}".')
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

        if tag is None:
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


class AttrParser:  # tag: <attr>
    @_tag_parser
    def __call__(self, tag: Optional[str], key: str, value):
        for attr in tag.split("."):
            value = getattr(value, attr)
        return key, value


ExtendMethod = Callable[[Any, Any], Any]
"""
~typing.Callable[[Any, Any], Any]: A method to merge two values into one.
"""


class RecursiveExtend:
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


class ExtendParser:  # tag: <extend>
    methods = {
        None: RecursiveExtend(op.add),
        "add": RecursiveExtend(op.add),
        "and": op.and_,
        "or": op.or_,
    }

    def __init__(self, methods: dict[str, ExtendMethod]):
        if methods:
            self.methods = methods | self.methods

    @_tag_parser
    def __call__(self, local: dict[str, Any], tag: Optional[str], key: str, value):
        if key not in local:
            return key, value
        try:
            return key, self.methods[tag](local[key], value)
        except KeyError:
            raise ValueError(
                f'When parsing "<{_ReservedTag.extend}={tag}>":\n  unknown method.'
            )


class VariableParser:  # tag: <var> <ref> <copy> <deepcopy>
    def __init__(self):
        self.local = {}

    @staticmethod
    def _get_name(*args):
        for arg in args:
            if isinstance(arg, str):
                return arg
        raise ValueError

    @_tag_parser
    def var(self, tag: Optional[str], key: str, value):
        try:
            self.local[self._get_name(tag, key)] = value
        except ValueError:
            raise ValueError(
                f'When parsing "<{_ReservedTag.var}>":\n  variable name cannot be None.'
            )
        return key, value

    def _get(self, tag: Optional[str], key: str, value, op: str):
        try:
            name = self._get_name(tag, value, key)
            return self.local[name]
        except KeyError:
            raise KeyError(
                f'When parsing "{key} <{op}={tag}>: {value}":\n  variable "{name}" does not exist.'
            )
        except ValueError:
            raise ValueError(f'Variable name cannot be None when parsing "<{op}>"')

    @_tag_parser
    def ref(self, tag: Optional[str], key: str, value):
        return key, self._get(tag, key, value, _ReservedTag.ref)

    @_tag_parser
    def copy(self, tag: Optional[str], key: str, value):
        return key, copy.copy(self._get(tag, key, value, _ReservedTag.copy))

    @_tag_parser
    def deepcopy(self, tag: Optional[str], key: str, value):
        return key, copy.deepcopy(self._get(tag, key, value, _ReservedTag.deepcopy))


def switch_parser(tag: str, tags: dict[str, Optional[str]], default: bool) -> bool:
    match tags.get(tag, 0):
        case 0:
            return default
        case "on":
            return True
        case "off":
            return False
        case _:
            raise ValueError(f'When parsing "<{tag}={tags[tag]}>":\n  unknown value')


class FileParser:  # tag: <file>
    @_tag_parser
    def __call__(
        self,
        path: str,
        tag: Optional[str],
        tags: dict[str, Optional[str]],
        key: str,
        value,
    ):
        use_cache = switch_parser(_ReservedTag.file_cache, tags, True)
        obj = next(
            load_url(
                partial(
                    ConfigParser.io.load,
                    use_cache=use_cache,
                ),
                next(resolve_path(path, tag, value)),
                parse_query=False,
            )
        )
        if use_cache:
            obj = copy.deepcopy(obj)
        return key, obj


class _Parser:
    __cwd = getcwd()

    def __init__(self, base: Optional[str], custom: _ParserInitializer):
        self.base = base or self.__cwd
        self.custom = custom

    def dict(self, data: dict[str, Any], singleton: bool = False, result: dict = None):
        if result is None:
            result = {}
        for k, v in data.items():
            key, tags, v = self.eval(k, v)
            if (include_tag := tags.get(_ReservedTag.include)) is not ...:
                if key is None:
                    self.include(v, include_tag, result=result)
                    continue
                else:
                    raise ValueError(f"Cannot use include with non-empty key: {key}")
            self.setitem(result, tags, key, v)
        if (
            singleton
            and len(result) == 1
            and None in result
            and not tags.has(_ReservedTag.literal)
        ):
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
        key, tags = self.custom.match(k)
        if tags.has(_ReservedTag.code):
            v = eval(v, None, self.custom.vars.local)
        return key, tags, v

    def setitem(self, local: dict[str, Any], tags: _MatchedTags, key: str, value: Any):
        if (
            self.custom.expand
            and key is not None
            and not tags.has(_ReservedTag.literal)
        ):
            keys = key.split(".")
            local = SimpleTree.init(local, keys[:-1])
            key = keys[-1]
        if isinstance(value, dict):
            value = self.dict(value)
        elif isinstance(value, list):
            value = self.list(value)
        key, value = tags.apply(path=self.base, local=local, key=key, value=value)
        if not tags.has(_ReservedTag.discard):
            local[key] = value

    def include(self, paths, tag: str, result: dict[str, Any] = None):
        if isinstance(paths, str):
            paths = [paths]
        if isinstance(paths, list) and all(isinstance(path, str) for path in paths):
            return self.custom.parse(
                *resolve_path(self.base, tag, *paths), result=result
            )
        raise ValueError(f"Cannot include the configs from: {paths}")


@dataclass
class _ParserCustomization:
    expand: bool = True
    custom_tags: dict[str, Optional[TagParser]] = field(default_factory=dict)
    extend_methods: dict[str, ExtendMethod] = field(default_factory=dict)


@dataclass
class _ParserInitializer(_ParserCustomization):
    pattern_match = re.compile(r"(?P<key>.*?)\s*(?P<tags>(\<[^\>\<]*\>\s*)*)\s*")
    pattern_split = re.compile(r"\<(?P<tag>[^\>\<]*)\>")

    static_parsers = {
        _ReservedTag.code: None,
        _ReservedTag.include: None,
        _ReservedTag.literal: None,
        _ReservedTag.discard: None,
        _ReservedTag.dummy: None,
        _ReservedTag.file: FileParser(),
        _ReservedTag.file_cache: None,
        _ReservedTag.type: TypeParser(),
        _ReservedTag.attr: AttrParser(),
    }

    def __post_init__(self):
        self.vars = VariableParser()
        self.parsers = (
            {k: v if v is None else _tag_parser(v) for k, v in self.custom_tags.items()}
            | self.static_parsers
            | {
                _ReservedTag.var: self.vars.var,
                _ReservedTag.ref: self.vars.ref,
                _ReservedTag.copy: self.vars.copy,
                _ReservedTag.deepcopy: self.vars.deepcopy,
                _ReservedTag.extend: ExtendParser(self.extend_methods),
            }
        )

    @classmethod
    def new(cls, other: _ParserCustomization):
        return cls(**{k.name: getattr(other, k.name) for k in fields(other)})

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
                configs = load_url(ConfigParser.io.load, path)
            else:
                configs = (configs,)
            parser = _Parser(path, self)
            for config in configs:
                if not isinstance(config, dict):
                    raise ValueError(f"Cannot parse non-dict config: {path}")
                parser.dict(config, result=result)
        return result

    def match(self, key: Optional[str]) -> tuple[Optional[str], _MatchedTags]:
        if key is None:
            return None, _NoTag
        matched = self.pattern_match.fullmatch(key)
        if not matched:
            return key, _NoTag
        tags = []
        for tag in self.pattern_split.finditer(matched["tags"]):
            k = tag["tag"].split("=")
            if len(k) == 1:
                v = None
            elif len(k) == 2:
                v = k[1]
            else:
                raise ValueError(f"Invalid tag format: <{tag}> in {key}")
            tags.append((k[0], v))
        key = matched["key"]
        if not key or key == "~":
            key = None
        return key, _MatchedTags(tags, self.parsers)


class ConfigParser(_ParserCustomization):
    """
    A customizable config parser.

    Parameters
    ----------
    expand : bool, optional, default=False
        Expand dot-separated keys to nested dicts.
    custom_tags : dict[str, Optional[TagParser]], optional
        Customized tags.
    extend_methods : dict[str, ExtendMethod], optional
        Customized <extend> methods.

    """

    io = FileLoader()
    """
    FileLoader: A config file loader with a shared cache.
    """

    def __call__(
        self, *path_or_dict: ConfigSource, result: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        """
        Load configs from multiple sources.

        Parameters
        ----------
        *path_or_dict : ConfigSource
            Dictionaries or paths to config files.
        result : dict[str, Any], optional
            If provided, the configs will be loaded into this dict.

        Returns
        -------
        dict[str, Any]
            The loaded configs.
        """
        return _ParserInitializer.new(self).parse(*path_or_dict, result=result)
