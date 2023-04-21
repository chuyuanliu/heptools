from __future__ import annotations

import operator
from typing import Any, Callable, Union

import awkward as ak
import numpy as np
from awkward import Array

from ._utils import astuple

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

def _operator_arrays(op: Callable[[Array, Array], Array], *arrays: Array) -> Array:
    result = None
    for array in arrays:
        if result is None:
            result = array
        else:
            result = op(result, array)
    return result

def mul_arrays(*arrays: Array):
    return _operator_arrays(operator.mul, *arrays)

def _operator_fields(data: Array, op: Callable[[Array, Array], Array], *fields: FieldLike) -> Array:
    return _operator_arrays(op, *[get_field(data, field) for field in fields])

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