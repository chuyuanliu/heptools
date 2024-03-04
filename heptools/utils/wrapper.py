from __future__ import annotations

import inspect
import time
from abc import ABC, abstractmethod
from functools import partial
from types import MethodType
from typing import Callable, Concatenate, Generic, Iterable, ParamSpec, TypeVar


class OptionalDecorator(ABC):
    @property
    @abstractmethod
    def _switch(cls) -> str:
        ...

    @abstractmethod
    def _decorate(cls, __func: Callable, **kwargs) -> Callable:
        ...

    def __new__(cls, __func=None, **kwargs):
        if __func is None:
            return partial(cls, **kwargs)
        return super().__new__(cls)

    def __init__(self, __func, **kwargs):
        signature = inspect.signature(__func)
        if self._switch not in signature.parameters:
            raise ValueError(
                f'Function "{__func.__name__}{signature}" must have a parameter "{self._switch}: bool"')
        self._func = __func
        self._decorated = None
        self._default = signature.parameters[self._switch].default
        if self._default is inspect.Signature.empty:
            self._default = False
        self._kwargs = kwargs

    def __get__(self, instance, owner):
        return MethodType(self, instance)

    def __call__(self, *args, **kwargs):
        kwargs.setdefault(self._switch, self._default)
        if kwargs[self._switch]:
            if self._decorated is None:
                self._decorated = self._decorate(self._func, **self._kwargs)
            return self._decorated(*args, **kwargs)
        else:
            return self._func(*args, **kwargs)


_RetryFuncT = TypeVar('_RetryFuncT', bound=Callable)
_RetryFuncP = ParamSpec('_RetryFuncP')
_RetryFuncReturnT = TypeVar('_RetryFuncReturnT')


class AutoRetry(Generic[_RetryFuncP, _RetryFuncReturnT]):
    def __init__(
        self,
        func: Callable[
            _RetryFuncP, _RetryFuncReturnT],
        max: int,
        delay: float = 0,
        handler: Callable[
            Concatenate[Exception, _RetryFuncP], _RetryFuncReturnT] = None,
        reset: Callable[
            _RetryFuncP, None] = None,
        skip: Iterable[Exception] = (),
    ):
        self._func = func
        self._max = max
        self._delay = delay
        self._handler = handler
        self._reset = reset
        self._skip = (*skip,)

    def set(
        self,
        max: int = ...,
        delay: float = ...,
    ):
        if max is not ...:
            self._max = max
        if delay is not ...:
            self._delay = delay

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return MethodType(self, instance)

    def __call__(self, *args: _RetryFuncP.args, **kwargs: _RetryFuncP.kwargs) -> _RetryFuncReturnT:
        for i in range(self._max):
            try:
                return self._func(*args, **kwargs)
            except Exception as e:
                if self._reset is not None:
                    self._reset(*args, **kwargs)
                if (i == self._max - 1) or any(isinstance(e, t) for t in self._skip):
                    if self._handler is not None:
                        return self._handler(e, *args, **kwargs)
                    else:
                        raise
                time.sleep(self._delay)


def retry(
    max: int,
    delay: float = 0,
    handler: Callable[
        Concatenate[Exception, _RetryFuncP], _RetryFuncReturnT] = None,
    reset: Callable[
        _RetryFuncP, None] = None,
    skip: Iterable[Exception] = (),
) -> Callable[[_RetryFuncT], _RetryFuncT]:
    return partial(
        AutoRetry,
        max=max,
        delay=delay,
        handler=handler,
        reset=reset,
        skip=skip,
    )
