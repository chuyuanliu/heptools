from __future__ import annotations

import json
from typing import TYPE_CHECKING, TypeVar

import awkward as ak
import numpy as np

from ..utils import seqcall
from ..utils.json import DefaultEncoder
from .structure import is_jagged

if TYPE_CHECKING:
    import numpy.typing as npt

# json

JSONableT = TypeVar("JSONableT")


def from_jsonable(*jsonables) -> ak.Array:
    return ak.Array(json.loads(json.dumps(jsonables, cls=DefaultEncoder)))


def to_jsonable(array: ak.Array, cls: type[JSONableT]) -> list[JSONableT]:
    array = ak.to_list(array)
    if hasattr(cls, "from_json"):
        return [cls.from_json(data) for data in array]
    else:
        return [cls(**data) for data in array]


# numpy

_copy_to_numpy = seqcall(np.array, ak.to_numpy)


def _numpy_array(
    array: ak.Array, copy: bool
) -> npt.NDArray | tuple[npt.NDArray, npt.NDArray]:
    convert = _copy_to_numpy if copy else ak.to_numpy
    if is_jagged(array):
        return convert(ak.flatten(array)), ak.to_numpy(ak.num(array))
    else:
        return convert(array)


def to_numpy(array: ak.Array, copy: bool = False):
    fields: list[str] = list(ak.fields(array))
    if fields:
        return {name: _numpy_array(array[name], copy) for name in fields}
    else:
        return _numpy_array(array, copy)
