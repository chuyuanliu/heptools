from __future__ import annotations

from functools import partial, reduce
from operator import add, and_, mul, or_
from typing import Any, Callable, Literal

import awkward as ak
import numpy as np
from awkward import Array

from .partition import Partition
from .utils import astuple

__all__ = ['FieldLike', 'AnyArray', 'RealNumber', 'AnyInt', 'AnyFloat',
           'has_record', 'get_field', 'set_field', 'update_fields', 'sort_field',
           'get_depth', 'get_typestr',
           'foreach', 'partition', 'where',
           'or_arrays', 'or_fields', 'and_arrays', 'and_fields', 'add_arrays', 'add_fields', 'mul_arrays', 'mul_arrays']

AnyInt    = int | np.integer
AnyFloat  = float | np.floating
RealNumber = AnyInt | AnyFloat
AnyArray  = Array | np.ndarray

# field

FieldLike = str | tuple

def has_record(data: Array, field: FieldLike) -> tuple[str, ...]:
    parents = []
    for level in astuple(field):
        try:
            data = data[level]
            parents.append(level)
        except:
            break
    return (*parents,)

def get_field(data: Array, field: FieldLike):
    if field is ...:
        try:
            return ak.num(data, axis = get_depth(data))
        except:
            return ak.Array(np.ones(len(data)))
    for level in astuple(field):
        data = getattr(data, level)
    return data

def set_field(data: Array, field: FieldLike, value: Array):
    field = astuple(field)
    parent = field[:len(has_record(data, field)) + 1]
    nested = field[len(parent):]
    if nested:
        for level in reversed(nested):
            value = ak.zip({level: value})
    data[parent] = value

def update_fields(data: Array, new: Array, *fields: FieldLike):
    if not fields:
        fields = new.fields
    for field in fields:
        set_field(data, field, get_field(new, field))

def sort_field(data: Array, field: FieldLike, axis: int = -1, ascending: bool = False) -> Array:
    return data[ak.argsort(get_field(data, field), axis = axis, ascending = ascending)]

# shape

def get_depth(data: Array, default: int = 0) -> int:
    try:
        return data.layout.minmax_depth[0]
    except AttributeError:
        return default

def get_typestr(data: Array, format: Literal['PascalCase', 'camelCase'] = 'PascalCase'):
    name = str(ak.type(data)).split(' * ')[-1]
    if format == 'PascalCase':
        name = name[0].upper() + name[1:]
    elif format == 'camelCase':
        name = name[0].lower() + name[1:]
    return name

# slice

def foreach(data: Array) -> tuple[Array, ...]:
    dim = get_depth(data) - 1
    count = np.unique(ak.ravel(ak.num(data, axis = dim)))
    if not len(count) == 1:
        raise IndexError(f'the length of the last axis must be uniform (got {count})')
    slices = tuple(slice(None) for _ in range(dim))
    return tuple(data[slices + (i,)] for i in range(count[0]))

def partition(data: Array, groups: int, members: int) -> tuple[Array, ...]:
    _sizes = ak.num(data)
    if not ak.all(_sizes >= groups * members):
        raise ValueError(f'not enough data to partition into {groups}×{members}')
    _combs = ak.Array([Partition(i, groups, members).combination[0] for i in range(ak.max(_sizes) + 1)])[_sizes]
    _combs = tuple(ak.unflatten(data[ak.flatten(_combs[:, :, :, i], axis = 2)], groups, axis=1) for i in range(members))
    return _combs

# reduce

def _op_arrays(*arrays: Array, op: Callable[[Array, Array], Array]) -> Array:
    if arrays:
        return reduce(op, arrays)

def _op_fields(data: Array, *fields: FieldLike, op: Callable[[Array, Array], Array]):
    return _op_arrays(*(get_field(data, field) for field in fields), op = op)

or_arrays = partial(_op_arrays, op = or_)
or_fields = partial(_op_fields, op = or_)
and_arrays = partial(_op_arrays, op = and_)
and_fields = partial(_op_fields, op = and_)
add_arrays = partial(_op_arrays, op = add)
add_fields = partial(_op_fields, op = add)
mul_arrays = partial(_op_arrays, op = mul)
mul_fields = partial(_op_fields, op = mul)

# where

def where(default: Array, *conditions: tuple[Array, Any]) -> Array:
    for condition, value in conditions:
        default = ak.where(condition, value, default)
    return default