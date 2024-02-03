import inspect
from abc import ABC, abstractmethod
from functools import partial
from types import MethodType
from typing import Callable


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
