from __future__ import annotations

from copy import deepcopy
from typing import Any, Generic, Optional, TypeVar, overload

from typing_extensions import Self

from ._manager import _status, _unset

T = TypeVar("T")


class Configurable:
    __config_namespace__: str
    __config_attrs__: frozenset[str] = frozenset()
    __config_cache__: Optional[dict[str, Any]]

    def __init_subclass__(cls, namespace: str = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if namespace is None:
            namespace = f"{cls.__module__}.{cls.__name__}"
        cls.__config_namespace__ = namespace
        cls.__config_cache__ = None
        for k, v in vars(cls).items():
            if isinstance(v, config):
                name = f"{namespace}.{k}"
                v._init(name)
        cls.__config_attrs__ = frozenset(
            v.name for k in dir(cls) if isinstance(v := getattr(cls, k), config)
        )

    def __new__(cls, *_, **__):
        self = super().__new__(cls)
        if _status.frozen:
            self.__config_cache__ = {
                k: deepcopy(_status.get(k)) for k in cls.__config_attrs__
            }
        return self


class config(Generic[T]):
    @overload
    def __init__(self, value: T): ...
    @overload
    def __init__(self, name: str): ...
    @overload
    def __init__(self, value: T, name: str): ...
    def __init__(self, value: T = _unset, /, name: str = _unset):
        self.__name = name
        self.__value = value

    @overload
    def __get__(self, instance: None, owner: type[Configurable]) -> Self[T]: ...
    @overload
    def __get__(self, instance: Configurable, owner: type[Configurable]) -> T: ...
    def __get__(self, instance: Configurable, _):
        if instance is None:
            return self
        if (cache := instance.__config_cache__) is not None:
            return cache[self.__name]
        return _status.get(self.__name)

    def __set__(self, instance: Configurable, value: T):
        if instance.__config_cache__ is not None:
            raise AttributeError("Cannot modify a frozen config.")
        self._set_data(self.__name, value)

    def _init(self, name: str):
        if self.__name is _unset:
            self.__name = name
        if self.__value is not _unset:
            self._init_data(self.__name, self.__value)
        del self.__value

    @staticmethod
    def _init_data(name: str, value: T):
        _status.default[name] = value

    @staticmethod
    def _set_data(name: str, value: T):
        _status.updated[name] = value

    @property
    def name(self):
        return self.__name

    @property
    def value(self):
        return _status.get(self.__name)

    def set(self, value: T):
        self._set_data(self.__name, value)

    def __repr__(self):
        return f"<{type(self).__name__}> {self.name} = {self.value}"


class const(config[T]):
    @staticmethod
    def _init_data(name: str, value: T):
        if name in _status.default:
            raise AttributeError(f"Constant {name} is initialized multiple times.")
        _status.default[name] = value

    @staticmethod
    def _set_data(name: str, _: T):
        raise AttributeError(f"Constant {name} cannot be modified at runtime.")
