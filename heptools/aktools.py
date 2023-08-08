from __future__ import annotations

import operator
from functools import partial, reduce
from typing import Any, Callable, Union

import awkward as ak
import numpy as np
from awkward import Array

from .utils import astuple
from .partition import Partition

__all__ = ['FieldLike', 'Sliceable',
           'get_field', 'update_fields', 'sort_field',
           'or_arrays', 'or_fields', 'and_arrays', 'and_fields', 'add_arrays', 'add_fields', 'mul_arrays', 'mul_arrays',
           'where', 'partition']

AnyInt = Union[int, np.integer]
AnyFloat = Union[float, np.floating]
AnyNumber = Union[AnyInt, AnyFloat]

FieldLike = Union[str, tuple]
Sliceable = Union[Array, np.ndarray]

def get_field(data: Array, field: FieldLike):
    fields = astuple(field)
    for field in fields:
        data = getattr(data, field)
    return data

def get_dimension(data: Array) -> int:
    try:
        return data.layout.minmax_depth[0]
    except AttributeError:
        return 0

def to_tuple(data: Array) -> tuple[Array, ...]:
    dim = get_dimension(data) - 1
    count = np.unique(ak.ravel(ak.num(data, axis = dim)))
    assert(len(count) == 1)
    slices = tuple(slice(None) for _ in range(dim))
    return tuple(data[slices + (i,)] for i in range(count[0]))

def update_fields(data: Array, new: Array, *fields: FieldLike):
    if not fields:
        fields = new.fields
    for field in fields:
        data[field] = get_field(new, field)

def sort_field(data: Array, field: FieldLike, axis: int = -1, ascending: bool = False) -> Array:
    return data[ak.argsort(get_field(data, field), axis = axis, ascending = ascending)]

def _op_arrays(*arrays: Array, op: Callable[[Array, Array], Array]) -> Array:
    if arrays:
        return reduce(op, arrays)

def _op_fields(data: Array, *fields: FieldLike, op: Callable[[Array, Array], Array]):
    return _op_arrays(*(get_field(data, field) for field in fields), op = op)

or_arrays = partial(_op_arrays, op = operator.or_)
or_fields = partial(_op_fields, op = operator.or_)
and_arrays = partial(_op_arrays, op = operator.and_)
and_fields = partial(_op_fields, op = operator.and_)
add_arrays = partial(_op_arrays, op = operator.add)
add_fields = partial(_op_fields, op = operator.add)
mul_arrays = partial(_op_arrays, op = operator.mul)
mul_fields = partial(_op_fields, op = operator.mul)

def where(default: Array, *conditions: tuple[Array, Any]) -> Array:
    for condition, value in conditions:
        default = ak.where(condition, value, default)
    return default

def partition(data: Array, groups: int, members: int) -> tuple[Array, ...]:
    _sizes = ak.num(data)
    assert(ak.all(_sizes >= groups * members))
    _combs = ak.Array([Partition(i, groups, members).combination[0] for i in range(ak.max(_sizes) + 1)])[_sizes]
    _combs = tuple(ak.unflatten(data[ak.flatten(_combs[:, :, :, i], axis = 2)], groups, axis=1) for i in range(members))
    return _combs