import json
from typing import Any, Protocol, runtime_checkable

__all__ = ['alias',
           'JSONable', 'DefaultEncoder']

def alias(*methods: str):
    def wrapper(cls):
        for method in methods:
            if not hasattr(cls, method):
                raise TypeError(f'`{cls.__name__}.{method}()` is not defined')
            setattr(cls, f'__{method}__', getattr(cls, method))
        return cls
    return wrapper

# json

@runtime_checkable
class JSONable(Protocol):
    def __json__(self) -> Any:
        ...

class DefaultEncoder(json.JSONEncoder):
    def default(self, __obj):
        if isinstance(__obj, JSONable):
            return __obj.__json__()
        return super().default(__obj)

# TODO print rich text