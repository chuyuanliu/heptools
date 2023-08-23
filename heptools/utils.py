from __future__ import annotations

import re
from typing import Any, Callable, Generic, Iterable, Sized, TypeVar

__all__ = ['sequence_call', 'astuple', 'unpack', 'merge_op', 'match_any', 'Eval']

def sequence_call(*_funcs: Callable[[Any], Any]):
    def func(x):
        for _func in _funcs:
            x = _func(x)
        return x
    return func

def astuple(_o):
    return _o if isinstance(_o, tuple) else (_o,)

def unpack(__iter: Iterable) -> Any:
    __next = __iter
    while isinstance(__next, Iterable) and isinstance(__next, Sized) and not isinstance(__next, str):
        if len(__next) == 1:
            __next = next(iter(__next))
        elif len(__next) == 0:
            return None
        else:
            return __iter
    return __next

def merge_op(op, _x, _y):
    if _x is None:
        return _y
    elif _y is None:
        return _x
    else:
        return op(_x, _y)

_TargetType = TypeVar('_TargetType')
_PatternType = TypeVar('_PatternType')
def match_any(target: _TargetType, patterns: _PatternType | Iterable[_PatternType], match: Callable[[_TargetType, _PatternType], bool]):
    if patterns is None:
        return False
    if patterns is ...:
        return True
    if not isinstance(patterns, Iterable) or isinstance(patterns, str):
        patterns = [patterns]
    for pattern in patterns:
        if match(target, pattern):
            return True
    return False

def ensure(__str: str, __prefix: str = None, __suffix: str = None):
    if __prefix is not None and not __str.startswith(__prefix):
        __str = __prefix + __str
    if __suffix is not None and not __str.endswith(__suffix):
        __str = __str + __suffix
    return __str

_EvalType = TypeVar('_EvalType')
class Eval(Generic[_EvalType]):
    _quote_arg_pattern = re.compile(r'(?P<arg>' +
                                    r'|'.join([rf'((?<={i})[^\[\]\",=]*?(?={j}))'
                                               for i in ['\[', ',']
                                               for j in [',', '\]']]) +
                                    r')')
    _eval_call_pattern = re.compile(r'\[(?P<arg>.*?)\]')

    def __init__(self, method: Callable[[], _EvalType] | dict[str, _EvalType], *args, **kwargs):
        self.method = method
        self.args   = args
        self.kwargs = kwargs

    def __call__(self, expression: str) -> _EvalType:
        return eval(re.sub(self._eval_call_pattern, rf'self.method(\g<arg>,*self.args,**self.kwargs)', re.sub(self._quote_arg_pattern, r'"\g<arg>"', expression)))

    def __getitem__(self, expression: str) -> _EvalType:
        return eval(re.sub(self._eval_call_pattern, rf'self.method[\g<arg>]', re.sub(self._quote_arg_pattern, r'"\g<arg>"', expression)))