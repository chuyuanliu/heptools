from __future__ import annotations

from typing import Any, get_type_hints

from rich.text import Text

from ._utils import isinstance_, type_name


class ConfigError(Exception):
    __module__ = Exception.__module__

Undefined = object()

class Config:
    __reserve__ = ['update', 'reset', 'report']
    __default__ = {}
    __unified__ = []
    __updated__ = {}

    @classmethod
    def __overridden__(cls):
        overridden = []
        class base(*cls.__bases__):
            ...
        for par in cls.__parameters__:
            if hasattr(base, par) and getattr(base, par) is not getattr(cls, par):
                overridden.append(par)
        return overridden

    @classmethod
    def __print_parameter__(cls, __par, __type = Any, __value = Undefined, __source = None):
        args = {} if __value is Undefined or isinstance_(__value, __type) else {'style': 'red'}
        if __source is None:
            __source = cls.__name__
        __source = Text(' (') + Text(f'{__source}', style = 'yellow') + Text(')') if __value is not Undefined else Text('')
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
            if not issubclass(config, cls):
                raise ConfigError(f'cannot update {cls.__name__} with {config.__name__}')
        for config in configs:
            diff = set(config.__bases__) - ({*cls.__bases__} | {cls} | {*cls.__unified__})
            if diff:
                raise ConfigError(f'cannot update {cls.__name__} with {config.__name__} without {diff}')
            cls.__unified__.append(config)
            for par in set(cls.__parameters__) & set(config.__overridden__()):
                v_old = getattr(cls, par)
                v_new = getattr(config, par)
                if v_old is not v_new:
                    setattr(cls, par, v_new)
                    cls.__updated__[par] = config.__name__
                    if par not in cls.__default__:
                        cls.__default__[par] = v_old

    @classmethod
    def reset(cls):
        for k, v in cls.__default__.items():
            setattr(cls, k, v)
        cls.__default__ = {}
        cls.__unified__ = []
        cls.__updated__ = {}

    @classmethod
    def report(cls):
        defined_pars = []
        undefined_pars = []
        pars = cls.__parameters__
        for __par in sorted(list(pars)):
            __value = getattr(cls, __par)
            __source = cls.__updated__.get(__par, None)
            __str = cls.__print_parameter__(__par, pars[__par], __value, __source)
            if __value is Undefined:
                undefined_pars.append(__str)
            else:
                defined_pars.append(__str)
        configs = " + ".join([i.__name__ for i in [*cls.__bases__] + [cls] + cls.__unified__])
        return Text('\n').join(
            [Text(f'{configs.replace("+", "=", 1)}', style = 'bold yellow')] +
            defined_pars + ((
            [Text('↓↓ Undefined ↓↓', style = 'yellow')] +
            undefined_pars +
            [Text('↑'*15, style = 'yellow')]) if undefined_pars else [])
        )