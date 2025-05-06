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
from textwrap import indent
from types import MappingProxyType, MethodType
from typing import Any, Callable, Iterable, Optional, ParamSpec, Protocol, TypeVar

from ._io import FileLoader, load_url, resolve_path
from ._utils import SimpleTree

P = ParamSpec("P")
T = TypeVar("T")

ConfigSource = str | PathLike | dict[str, Any]
"""
str, ~os.PathLike, dict: A path to the config file or a nested dict.
"""


def _error_msg(error: Exception, key: str, value=..., tag: str = ...) -> str:
    if tag is ...:
        head = "When parsing"
    else:
        head = f"When parsing <{tag}> in"
    if value is ...:
        value = ""
    else:
        value = indent(repr(value), "    ") + "\n"
    return f"""

{head}
  {key}: 
{value}
The following error occurred:
  {type(error).__name__}:
{indent(str(error), "    ")}
"""


class _ReservedTag(StrEnum):
    def _generate_next_value_(name, *_):
        return name.lower().replace("_", "-")

    code = auto()

    include = auto()
    patch = auto()  # TODO add
    install = auto()  # TODO add
    uninstall = auto()  # TODO add

    literal = auto()
    discard = auto()
    dummy = auto()

    file = auto()
    type = auto()
    key_type = auto()
    value_type = auto()
    attr = auto()
    extend = auto()
    var = auto()
    ref = auto()
    copy = auto()
    deepcopy = auto()


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
    flags = frozenset(
        (
            _ReservedTag.code,
            _ReservedTag.literal,
            _ReservedTag.discard,
        )
    )
    uniques = frozenset(
        (
            _ReservedTag.include,
            _ReservedTag.patch,
            _ReservedTag.install,
            _ReservedTag.uninstall,
        )
    )

    def __init__(
        self,
        raw: str,
        tags: list[tuple[str, str]],
        parsers: dict[str, Optional[TagParser]],
    ):
        self.raw_key = raw
        self.parsers = parsers
        self.tags = list[tuple[str, str]]()
        self.parsed = dict[str, str]()
        self.unique: Optional[tuple[str, str]] = None

        unique_check = 0

        for k, v in tags:
            if k != _ReservedTag.code:
                unique_check += 1
            if k in self.flags:
                self.parsed[k] = v
            elif k in self.uniques:
                self.unique = (k, v)
            else:
                self.tags.append((k, v))

        if self.unique is not None and unique_check > 1:
            raise SyntaxError(
                _error_msg(
                    error=ValueError(f"cannot use <{self.unique[0]}> with other tags"),
                    key=self.raw_key,
                )
            )

    def has(self, tag: str):
        return tag in self.parsed

    def get(self, tag: str):
        return self.parsed.get(tag, ...)

    def apply(self, *, key: str, value, **kwargs):
        raw_value = value
        for tag_k, tag_v in self.tags:
            if (parser := self.parsers.get(tag_k)) is not None:
                try:
                    key, value = parser(
                        key=key,
                        value=value,
                        tag=tag_v,
                        tags=MappingProxyType(self.parsed),
                        **kwargs,
                    )
                except RecursionError:
                    raise
                except Exception as e:
                    raise SyntaxError(
                        _error_msg(
                            key=self.raw_key,
                            value=raw_value,
                            error=e,
                            tag=tag_k,
                        )
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

    def _new(self, tag: Optional[str], value: Any):
        # import module
        if tag is None:
            if not isinstance(value, str):
                raise ValueError("type name must be a string.")
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
                raise ImportError(f'invalid import format "{fullname}".')
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
            return cls

        # parse args and kwargs
        if isinstance(value, dict):
            kwargs = value.copy()
            args = kwargs.pop(None, [])
        else:
            kwargs = {}
            args = value
        if not isinstance(args, list):
            args = [args]
        return cls(*args, **kwargs)

    @_tag_parser
    def __call__(self, tag: Optional[str], key: str, value: Any):
        return key, self._new(tag, value)


class KeyTypeParser(TypeParser):  # tag: <key>
    @_tag_parser
    def __call__(self, tag: Optional[str], key: str, value: Any):
        return self._new(tag, key), value


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
            raise ValueError(f'unknown extend method "{tag}".')


class VariableParser:  # tag: <var> <ref> <copy> <deepcopy>
    def __init__(self):
        self.local = {}

    @staticmethod
    def _get_name(*args):
        for arg in args:
            if isinstance(arg, str):
                return arg
        raise ValueError("variable name cannot be None.")

    @_tag_parser
    def var(self, tag: Optional[str], key: str, value):
        self.local[self._get_name(tag, key)] = value
        return key, value

    def _get(self, tag: Optional[str], key: str, value):
        try:
            name = self._get_name(tag, value, key)
            return self.local[name]
        except KeyError:
            raise KeyError(f'variable "{name}" does not exist.')
        except Exception:
            raise

    @_tag_parser
    def ref(self, tag: Optional[str], key: str, value):
        return key, self._get(tag, key, value)

    @_tag_parser
    def copy(self, tag: Optional[str], key: str, value):
        return key, copy.copy(self._get(tag, key, value))

    @_tag_parser
    def deepcopy(self, tag: Optional[str], key: str, value):
        return key, copy.deepcopy(self._get(tag, key, value))


class FlagParser:
    def __init__(self, *booleans, **enums: Iterable[str]):
        self._flags: dict[str, Optional[bool | str]] = {}
        self._categories: dict[str, str] = {}
        for k, vs in enums.items():
            self._flags[k] = None
            for v in vs:
                self._categories[v] = k
        for f in booleans:
            self._flags[f] = False
            self._categories[f] = ...

    def __call__(self, flags: list[str]):
        parsed = self._flags.copy()
        for flag in flags:
            if cat := self._categories.get(flag):
                if cat == ...:
                    parsed[flag] = True
                elif parsed[cat] is not None:
                    raise ValueError(
                        f'flag "{flag}" and "{parsed[cat]}" cannot be used together.'
                    )
                else:
                    parsed[cat] = flag
            else:
                raise ValueError(f'unknown flag "{flag}".')
        return parsed


class FileParser:  # tag: <file>
    __flags = FlagParser("nocache", "nobuffer", path=("relative", "absolute"))

    @_tag_parser
    def __call__(
        self,
        path: str,
        tag: Optional[str],
        key: str,
        value,
    ):
        flags = self.__flags(() if tag is None else tag.split("|"))
        use_cache = not flags["nocache"]
        obj = next(
            load_url(
                partial(
                    ConfigParser.io.load,
                    use_cache=use_cache,
                    use_buffer=not flags["nobuffer"],
                ),
                next(resolve_path(path, flags["path"], value)),
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
            match tags.unique:
                case ("include", tag):
                    self.include(v, tag, result=result)
                case ("patch", tag):
                    ...  # TODO
                case ("install", tag):
                    ...  # TODO
                case ("uninstall", tag):
                    ...  # TODO
                case None:
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
            try:
                v = eval(v, None, self.custom.vars.local)
            except Exception as e:
                raise SyntaxError(_error_msg(k, v, e))
        return key, tags, v

    def setitem(self, local: dict[str, Any], tags: _MatchedTags, key: str, value: Any):
        if (
            self.custom.nested
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
            try:
                return self.custom.parse(
                    *resolve_path(self.base, tag, *paths), result=result
                )
            except RecursionError:
                raise RecursionError(
                    "\n".join(
                        [
                            "Recursion may exist in",
                            f"  {self.base}",
                            "when <include>:",
                            *(f"  - {p}" for p in paths),
                        ]
                    )
                )
        raise SyntaxError(f"Invalid include path: {paths}")


@dataclass
class _ParserCustomization:
    nested: bool = True
    custom_tags: dict[str, Optional[TagParser]] = field(default_factory=dict)
    extend_methods: dict[str, ExtendMethod] = field(default_factory=dict)


@dataclass
class _ParserInitializer(_ParserCustomization):
    pattern_match = re.compile(r"(?P<key>.*?)\s*(?P<tags>(\<[^\>\<]*\>\s*)*)\s*")
    pattern_split = re.compile(r"\<(?P<tag>[^\>\<]*)\>")

    __type = TypeParser()
    static_parsers = {
        _ReservedTag.code: None,
        _ReservedTag.include: None,
        _ReservedTag.literal: None,
        _ReservedTag.discard: None,
        _ReservedTag.dummy: None,
        _ReservedTag.file: FileParser(),
        _ReservedTag.type: __type,
        _ReservedTag.value_type: __type,
        _ReservedTag.key_type: KeyTypeParser(),
        _ReservedTag.attr: AttrParser(),
    }

    def __post_init__(self):
        self.vars = VariableParser()
        self.parsers = (
            {k: v if v is None else _tag_parser(v) for k, v in self.custom_tags.items()}
            | self.static_parsers
            | {
                _ReservedTag.extend: ExtendParser(self.extend_methods),
                _ReservedTag.var: self.vars.var,
                _ReservedTag.ref: self.vars.ref,
                _ReservedTag.copy: self.vars.copy,
                _ReservedTag.deepcopy: self.vars.deepcopy,
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
                    raise SyntaxError(f"Cannot parse non-dict config: {path}")
                parser.dict(config, result=result)
        return result

    def match(self, raw: Optional[str]) -> tuple[Optional[str], _MatchedTags]:
        if raw is None:
            return None, _NoTag
        matched = self.pattern_match.fullmatch(raw)
        if not matched:
            return raw, _NoTag
        tags = []
        for tag in self.pattern_split.finditer(matched["tags"]):
            k = tag["tag"].split("=")
            if len(k) == 1:
                v = None
            elif len(k) == 2:
                v = k[1]
            else:
                raise SyntaxError(f"Invalid tag format: <{tag}> in {raw}")
            tags.append((k[0], v))
        key = matched["key"]
        if not key or key == "~":
            key = None
        return key, _MatchedTags(raw, tags, self.parsers)


class ConfigParser(_ParserCustomization):
    """
    A customizable config parser.

    Parameters
    ----------
    nested : bool, optional, default=True
        Parse dot-separated keys to nested dicts.
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
