from __future__ import annotations

import operator
from functools import reduce
from typing import Any, Callable, Union

import awkward as ak
import numpy as np
from awkward import Array

from ._utils import astuple
from .partition import Partition

__all__ = ['FieldLike', 'Sliceable',
           'get_field', 'update_fields', 'sort_field',
           'mul_arrays',
           'or_fields', 'and_fields', 'mul_fields',
           'where']

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

def update_fields(data: Array, new: Array, *fields: FieldLike):
    if not fields:
        fields = new.fields
    for field in fields:
        data[field] = get_field(new, field)

def sort_field(data: Array, field: FieldLike, axis: int = -1, ascending: bool = False):
    return data[ak.argsort(get_field(data, field), axis = axis, ascending = ascending)]

def _reduce(op: Callable[[Array, Array], Array], *arrays: Array) -> Array:
    if arrays:
        return reduce(op, arrays)

def mul_arrays(*arrays: Array):
    return _reduce(operator.mul, *arrays)

def _operator_fields(data: Array, op: Callable[[Array, Array], Array], *fields: FieldLike) -> Array:
    return _reduce(op, *[get_field(data, field) for field in fields])

def or_fields(data: Array, *fields: FieldLike):
    return _operator_fields(data, operator.or_, *fields)

def and_fields(data: Array, *fields: FieldLike):
    return _operator_fields(data, operator.and_, *fields)

def mul_fields(data: Array, *fields: FieldLike):
    return _operator_fields(data, operator.mul, *fields)

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