from __future__ import annotations

from dataclasses import dataclass
from functools import partial, wraps
from typing import Callable, Generic, Optional, ParamSpec, Protocol, TypeVar, overload

import awkward as ak
import dask_awkward as dak
import dask_awkward.lib.core as dakcore
from dask.base import unpack_collections

from ._utils import is_typetracer

T = TypeVar("T")
P = ParamSpec("P")


@dataclass
class _RepackWrapper:
    fn: Callable
    args: Callable
    kwargs: Callable
    division: int
    meta: Optional[Callable]

    def __call__(self, *collections):
        fn = self.fn
        if self.meta is not None and any(is_typetracer(c) for c in collections):
            fn = self.meta
        return fn(
            *self.args(collections[: self.division])[0],
            **self.kwargs(collections[self.division :])[0],
        )


class _DelayedDecorator(Protocol):
    def __call__(self, func: Callable[P, T]) -> _DelayedWrapper[P, T]: ...


@dataclass
class _DelayedWrapper(Generic[P, T]):
    func: Callable[P, T]
    label: Optional[str] = None
    token: Optional[str] = None
    meta: Callable[P, ak.Array] = None
    output_divisions: Optional[int] = None
    traverse: bool = True

    def __post_init__(self):
        if self.label is None:
            self.label = self.func.__name__

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T | dak.Array:
        arg_col, arg_repack = unpack_collections(args, traverse=self.traverse)
        kwarg_col, kwarg_repack = unpack_collections(kwargs, traverse=self.traverse)
        if (len(arg_col) + len(kwarg_col)) == 0:
            return self.func(*args, **kwargs)
        func = _RepackWrapper(
            fn=self.func,
            args=arg_repack,
            kwargs=kwarg_repack,
            division=len(arg_col),
            meta=self.meta,
        )
        return dakcore._map_partitions(
            func,
            *arg_col,
            *kwarg_col,
            label=self.label,
            token=self.token,
            meta=None,
            output_divisions=self.output_divisions,
        )


@overload
def delayed(
    func: Callable[P, T],
    /,
    typehint: None = None,
    label: Optional[str] = None,
    token: Optional[str] = None,
    meta: Optional[Callable[P, ak.Array]] = None,
    output_divisions: Optional[int] = None,
    traverse: bool = True,
) -> _DelayedWrapper[P, T]: ...
@overload
def delayed(
    func: None = None,
    /,
    typehint: None = None,
    label: Optional[str] = None,
    token: Optional[str] = None,
    meta: Optional[Callable[P, ak.Array]] = None,
    output_divisions: Optional[int] = None,
    traverse: bool = True,
) -> _DelayedDecorator: ...
@overload
def delayed(
    func: None = None,
    /,
    typehint: Callable[P, T] = ...,
    label: Optional[str] = None,
    token: Optional[str] = None,
    meta: Optional[Callable[P, ak.Array]] = None,
    output_divisions: Optional[int] = None,
    traverse: bool = True,
) -> Callable[[Callable], _DelayedWrapper[P, T]]: ...
def delayed(
    func=None,
    /,
    typehint=None,
    **kwargs,
):
    if func is None:
        return partial(delayed, typehint=typehint, **kwargs)
    else:
        if typehint is None:
            typehint = func
        return wraps(typehint)(_DelayedWrapper(func, **kwargs))
