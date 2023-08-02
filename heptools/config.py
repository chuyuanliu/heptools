from __future__ import annotations

from functools import partial
from inspect import getmro
from typing import Any, Callable, get_type_hints

from rich.text import Text

from ._utils import isinstance_, type_name


class ConfigError(Exception):
    __module__ = Exception.__module__

Undefined = object()
Extra = object()

class config_property:
    def __new__(cls, *dependencies: str):
        if isinstance(dependencies[-1], Callable):
            func, args = dependencies[-1], dependencies[:-1]
            self = object.__new__(cls)
            self._dependencies = args
            self._func = func
            return self
        else:
            return partial(cls, *dependencies)
    def __get__(self, _, cls: type[Config]):
        for dependency in self._dependencies:
            if (not hasattr(cls, dependency)) or getattr(cls, dependency) is Undefined:
                return Undefined
        return self._func(cls)

class Config:
    __annotations__ = {}

    __reserve__ = ['update', 'reset', 'report', 'undefined']
    __unified__ = []
    __default__ = {}
    __updated__ = {}

    @classmethod
    def __overridden__(cls):
        overridden = {}
        class base(*cls.__bases__):
            ...
        for k, v in cls.__parameters__.items():
            if (not hasattr(base, k)) or getattr(base, k) is not getattr(cls, k):
                overridden[k] = v
        return overridden

    @classmethod
    def __print_parameter__(cls, __par, __type = Any, __value = Undefined):
        args = {} if __value is Undefined or isinstance_(__value, __type) else {'style': 'red'}
        __source = cls.__updated__.get(__par, cls.__name__)
        __extra  = cls.__default__.get(__par) is Extra
        __source = Text(' (') + Text(f'{__source}', style = 'yellow' + (' italic' if __extra else '')) + Text(')') if (__value is not Undefined) or __extra else Text('')
        __type   = Text(' : ') + Text(f'{type_name(__type)}', style = 'green') if __type is not Any else Text('')
        __value  = Text(' = ') + Text(f'{__value}', style = 'blue') if __value is not Undefined else Text('')
        return Text(f'{__par}', **args) + __type + __value + __source

    @classmethod
    @property
    def __parameters__(cls):
        keys = set(dir(cls)) - set(cls.__reserve__)
        hints = get_type_hints(cls)
        return {k: hints.get(k, Any) for k in keys if not ((k.startswith('__') and k.endswith('__')))}

    @classmethod
    def update(cls, *configs: Config):
        for config in configs:
            diff = set(config.__bases__) - ({*getmro(cls)} | {*cls.__unified__})
            if diff:
                raise ConfigError(f'cannot update {cls.__name__} with {config.__name__} without {",".join([i.__name__ for i in diff])}')
            cls.__unified__.append(config)
            for k, v in config.__overridden__().items():
                v_new = getattr(config, k)
                if hasattr(cls, k):
                    v_old = getattr(cls, k)
                    if v_old is not v_new:
                        setattr(cls, k, v_new)
                        cls.__updated__[k] = config.__name__
                        if k not in cls.__default__:
                            cls.__default__[k] = v_old
                else:
                    setattr(cls, k, v_new)
                    cls.__updated__[k] = config.__name__
                    cls.__default__[k] = Extra
                    cls.__annotations__[k] = v

    @classmethod
    def reset(cls):
        for k, v in cls.__default__.items():
            if v is Extra:
                delattr(cls, k)
                cls.__annotations__.pop(k, None)
            else:
                setattr(cls, k, v)
        cls.__unified__ = []
        cls.__default__ = {}
        cls.__updated__ = {}

    @classmethod
    def report(cls):
        defined_pars = []
        undefined_pars = []
        pars = cls.__parameters__
        for __par in sorted([*pars]):
            __value = getattr(cls, __par)
            __str = cls.__print_parameter__(__par, pars[__par], __value)
            if __value is Undefined:
                undefined_pars.append(__str)
            else:
                defined_pars.append(__str)
        configs = [config.__name__ for config in [*getmro(cls)][::-1] + cls.__unified__ if config not in [object, Config]]
        configs = ' + '.join(configs)
        if configs:
            configs = ' = ' + configs
        return Text('\n').join(
            [Text(f'Config{configs}', style = 'bold yellow')] +
            defined_pars + ((
            [Text('↓↓ Undefined ↓↓', style = 'yellow')] +
            undefined_pars +
            [Text('↑'*15, style = 'yellow')]) if undefined_pars else [])
        )

    @classmethod
    @property
    def undefined(cls):
        return [__par for __par in cls.__parameters__ if getattr(cls, __par) is Undefined]