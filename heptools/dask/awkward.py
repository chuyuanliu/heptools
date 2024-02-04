from functools import partial, wraps
from itertools import chain
from typing import Callable, TypeVar

import awkward as ak
import dask_awkward as dak

_MapFuncT = TypeVar('_MapFuncT')


class _MapPartitions:
    def __init__(self, __func: Callable = None, shape: ak.Array | Callable[[], ak.Array] = ...):
        self._func = __func
        self._shape = (
            shape if shape is not isinstance(shape, ak.Array)
            else ak.Array(shape.layout.to_typetracer(forget_length=True)))

    def _wrapper(self, *args, **kwargs):
        for arg in chain(args, kwargs.values()):
            if isinstance(arg, ak.Array):
                if ak.backend(arg) == 'typetracer':
                    if self._shape is ...:
                        return arg
                    elif isinstance(self._shape, ak.Array):
                        return self._shape
                    else:
                        return self._shape(*args, **kwargs)
        return self._func(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return dak.map_partitions(self._wrapper, *args, **kwargs)


def map_partitions(__func: _MapFuncT = None, shape: ak.Array | Callable[[], ak.Array] = ...) -> _MapFuncT:
    if __func is None:
        return partial(map_partitions, shape=shape)
    else:
        return wraps(__func)(_MapPartitions(__func, shape))
