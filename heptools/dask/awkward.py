from functools import partial, wraps
from itertools import chain
from typing import Callable, TypeVar

import awkward as ak
import dask_awkward as dak

from .. import awkward as akext

_DelayedFuncT = TypeVar('_DelayedFuncT')


class _Delayed:
    def __init__(self, __func: Callable = None, mock: ak.Array | Callable[[], ak.Array] = ...):
        self._func = __func
        self._mock = (
            mock if mock is not isinstance(mock, ak.Array)
            else ak.Array(mock.layout.to_typetracer(forget_length=True)))

    def _wrapper(self, *args, **kwargs):
        for arg in chain(args, kwargs.values()):
            if isinstance(arg, ak.Array):
                if ak.backend(arg) == 'typetracer':
                    if self._mock is ...:
                        return arg
                    elif isinstance(self._mock, ak.Array):
                        return self._mock
                    else:
                        return self._mock(*args, **kwargs)
        return self._func(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        for arg in chain(args, kwargs.values()):
            if isinstance(arg, dak.Array):
                return dak.map_partitions(self._wrapper, *args, **kwargs)
        return self._func(*args, **kwargs)


def delayed(__func: _DelayedFuncT = None, shape: ak.Array | Callable[[], ak.Array] = ...) -> _DelayedFuncT:
    if __func is None:
        return partial(delayed, shape=shape)
    else:
        return wraps(__func)(_Delayed(__func, shape))


def from_jsonable(*jsonables, npartitions: int = ...) -> dak.Array:
    if npartitions is ...:
        npartitions = len(jsonables)
    return dak.from_awkward(akext.from_.jsonable(*jsonables), npartitions=npartitions)
