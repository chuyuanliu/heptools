from __future__ import annotations

from typing import Any, Callable, get_args

__all__ = ['isinstance_', 'sequence_call', 'astuple']

def isinstance_(__obj, __class_or_tuple) -> bool:
    try:
        return isinstance(__obj, __class_or_tuple)
    except:
        return isinstance(__obj, get_args(__class_or_tuple))

def sequence_call(*_funcs: Callable[[Any], Any]):
    def func(x):
        for _func in _funcs:
            x = _func(x)
        return x
    return func

def astuple(_o):
    return _o if isinstance(_o, tuple) else (_o,)