from __future__ import annotations

from copy import copy
from inspect import getmro
from typing import Any, Generic, TypeVar, get_args, get_origin, get_type_hints

from rich.text import Text

from .utils import isinstance_, type_name

__all__ = ['Config', 'ConfigError',
           'config_property',
           'Undefined', 'Extra']

class ConfigError(Exception):
    __module__ = Exception.__module__

Undefined = object()
Extra = object()

_ConfigPropertyType = TypeVar('_ConfigPropertyType')
class config_property(Generic[_ConfigPropertyType]):
    def __init__(self, func):
        self._func = func
        self._type = get_type_hints(func).get('return', Any)
    def __get__(self, _, cls: type[Config]) -> _ConfigPropertyType:
        try:
            return self._func(cls)
        except:
            return Undefined

class ConfigMeta(type):
    __reserved__ = {
                '__unified__': list,
                '__default__': dict,
                '__updated__': dict,
            }
    def __getattr__(cls, key):
        if key in cls.__reserved__:
            _key = f'__{cls.__name__.lower()}{key}'
            if _key not in vars(cls):
                setattr(cls, _key, cls.__reserved__[key]())
            return getattr(cls, _key)
        raise AttributeError

class Config(metaclass = ConfigMeta):
    def _protected(func):
        def wrapper(cls, *args, **kwargs):
            if cls is Config:
                raise ConfigError(f'cannot call "Config.{func.__name__}()" from base class')
            return func(cls, *args, **kwargs)
        return wrapper

    @_protected
    def __new__(cls):
        pars = cls.__get_parameter__(False)
        return pars | {
            '__mro__'    : getmro(cls)[1:],
            '__unified__': [cls] + copy(cls.__unified__),
            '__updated__': {k: cls.__track_parameter__(k) for k in pars},
        }

    @classmethod
    def __print_parameter__(cls, __par, __type, __value):
        if get_origin(__type) is config_property:
            __type = get_args(__type)[0]
        args = {} if __value is Undefined or isinstance_(__value, __type) else {'style': 'red'}
        __source = cls.__track_parameter__(__par)
        __extra  = cls.__default__.get(__par) is Extra
        __source = Text(' (') + Text(f'{__source}', style = 'yellow' + (' italic' if __extra else '')) + Text(')') if (__value is not Undefined) or __extra else Text('')
        __type   = Text(' : ') + Text(f'{type_name(__type)}', style = 'green') if __type is not Any else Text('')
        if __value is Undefined:
            __value = Text('')
        else:
            __value = f'{__value}'
            if '\n' in __value:
                __value = '\n' + __value
            __value = Text(' = ') + Text(f'{__value}', style = 'blue')
        return Text(f'{__par}', **args) + __type + __value + __source

    @classmethod
    def __set_parameter__(cls, __par, __type, __value):
        if hasattr(cls, __par):
            v_old = getattr(cls, __par)
            if v_old is not __par:
                setattr(cls, __par, __value)
                if __par not in cls.__default__:
                    cls.__default__[__par] = v_old
                return True
        else:
            setattr(cls, __par, __value)
            cls.__default__[__par] = Extra
            if not isinstance(__value, config_property):
                cls.__annotations__[__par] = __type
            return True
        return False

    @classmethod
    def __get_parameter__(cls, property_value = True, derived_only = False):
        hints = get_type_hints(cls)
        pars = {}
        configs = (cls,) if derived_only else getmro(cls)
        for config in configs:
            if config is Config:
                break
            for k, v in vars(config).items():
                if not (k.startswith('__') and k.endswith('__')):
                    if isinstance(v, config_property):
                        t = v._type
                        if property_value:
                            v = getattr(cls, k)
                    else:
                        t = hints.get(k, Any)
                    pars[k] = (t, v)
        return pars

    @classmethod
    def __track_parameter__(cls, __par):
        try:
            return cls.__updated__[__par]
        except KeyError:
            for config in getmro(cls):
                if config is Config:
                    break
                if __par in vars(config):
                    return config.__name__
        return ''

    @classmethod
    @_protected
    def update(cls, *configs: type[Config] | dict):
        for config in configs:
            if isinstance_(config, type[Config]):
                _mro  = getmro(config)[1:]
                _unified = [config] + config.__unified__
                _updated = config.__updated__
                _name = config.__name__
                _pars = config.__get_parameter__(False, True)
            elif isinstance_(config, dict):
                config = copy(config)
                _mro  = config.pop('__mro__', ())
                _unified = config.pop('__unified__', [])
                _updated = config.pop('__updated__', {})
                _name = _unified[0].__name__
                _pars = config
            diff = {*_mro} - ({*getmro(cls)} | {*cls.__unified__})
            if diff:
                raise ConfigError(f'cannot update <{cls.__name__}> with <{_name}> without [{",".join([i.__name__ for i in diff])}]')
            cls.__unified__.extend(_unified)
            for k, (t, v) in _pars.items():
                if cls.__set_parameter__(k, t, v):
                    cls.__updated__[k] = _updated.get(k, _name)

    @classmethod
    @_protected
    def reset(cls):
        for k, v in cls.__default__.items():
            if v is Extra:
                delattr(cls, k)
                cls.__annotations__.pop(k, None)
            else:
                setattr(cls, k, v)
        cls.__unified__.clear()
        cls.__default__.clear()
        cls.__updated__.clear()

    @classmethod
    @_protected
    def report(cls): # TODO rename __repr__
        defined_pars = []
        undefined_pars = []
        pars = cls.__get_parameter__()
        for k in sorted([*pars]):
            t, v = pars[k]
            __str = cls.__print_parameter__(k, t, v)
            if v is Undefined:
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
    @_protected
    def undefined(cls):
        return [k for k, (_, v) in cls.__get_parameter__().items() if v is Undefined]