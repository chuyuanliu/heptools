from __future__ import annotations

from typing import Any, get_type_hints

from rich.text import Text

from ._utils import isinstance_, type_name


class ConfigError(Exception):
    __module__ = Exception.__module__

Undefined = object()

class Config:
    __default__ = {}
    __reserve__ = ['update', 'reset', 'report']
    __unified__ = set()

    @classmethod
    def __print_parameter__(cls, __par, __type = Any, __value = Undefined):
        __match = __value is Undefined or isinstance_(__value, __type)
        args = {} if __match else {'style': 'red'}
        __type  = Text(' : ') + Text(f'{type_name(__type)}', style = 'green') if __type is not Any else Text('')
        __value = Text(' = ') + Text(f'{__value}', style = 'blue') if __value is not Undefined else Text('')
        return Text(f'{__par}', **args) + __type + __value

    @classmethod
    @property
    def __parameters__(cls):
        keys = set(dir(cls)) - set(cls.__reserve__)
        hints = get_type_hints(cls)
        return {k: hints.get(k, Any) for k in keys if not ((k.startswith('__') and k.endswith('__')))}

    @classmethod
    def update(cls, *configs):
        for config in configs:
            if not issubclass(config, cls):
                raise ConfigError(f'cannot update {cls.__name__} with {config.__name__}')
        for config in configs:
            cls.__unified__.add(config.__name__)
            for par in cls.__parameters__:
                v_old = getattr(cls, par)
                v_new = getattr(config, par)
                if v_old != v_new:
                    setattr(cls, par, v_new)
                    if par not in cls.__default__:
                        cls.__default__[par] = v_old

    @classmethod
    def reset(cls):
        for k, v in cls.__default__.items():
            setattr(cls, k, v)
        cls.__default__ = {}
        cls.__unified__ = set()

    @classmethod
    def report(cls):
        defined_pars = []
        undefined_pars = []
        pars = cls.__parameters__
        for __par in sorted(list(pars)):
            __value = getattr(cls, __par)
            __str = cls.__print_parameter__(__par, pars[__par], __value)
            if __value is Undefined:
                undefined_pars.append(__str)
            else:
                defined_pars.append(__str)
        configs = " + ".join([cls.__name__] + sorted(list(cls.__unified__)))
        return Text('\n').join(
            [Text(f'Config = {configs}', style = 'bold yellow')] +
            defined_pars + ((
            [Text('↓↓ Undefined ↓↓', style = 'yellow')] +
            undefined_pars +
            [Text('↑'*15, style = 'yellow')]) if undefined_pars else [])
        )