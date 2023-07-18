from __future__ import annotations


class ConfigError(Exception):
    __module__ = Exception.__module__

class Config:
    __default__ = {}
    __reserve__ = ['update', 'reset']

    @classmethod
    def update(cls, *configs):
        for config in configs:
            if not issubclass(config, cls):
                raise ConfigError(f'Cannot update {cls.__name__} with {config.__name__}')
        for config in configs:
            keys = set(dir(cls)) - set(cls.__reserve__)
            for k in keys:
                if not (k.startswith('__') and k.endswith('__')):
                    v_old = getattr(cls, k)
                    v_new = getattr(config, k)
                    if v_old != v_new:
                        setattr(cls, k, v_new)
                        if k not in cls.__default__:
                            cls.__default__[k] = v_old

    @classmethod
    def reset(cls):
        for k, v in cls.__default__.items():
            setattr(cls, k, v)
        cls.__default__ = {}