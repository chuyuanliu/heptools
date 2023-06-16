from __future__ import annotations

import re
from typing import Any, Callable, Generic, TypeVar, get_args

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