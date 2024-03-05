from typing import TypeVar

import awkward as ak
import numpy.typing as npt

from .is_ import jagged as is_jagged


def _numpy_array(array: ak.Array) -> npt.NDArray | tuple[npt.NDArray, npt.NDArray]:
    if is_jagged(array):
        return ak.to_numpy(ak.flatten(array)), ak.to_numpy(ak.num(array))
    else:
        return ak.to_numpy(array)


def numpy(array: ak.Array):
    fields: list[str] = list(ak.fields(array))
    if fields:
        return {name: _numpy_array(array[name]) for name in fields}
    else:
        return _numpy_array(array)


JSONableT = TypeVar('JSONableT')


def jsonable(array: ak.Array, cls: type[JSONableT]) -> list[JSONableT]:
    array = ak.to_list(array)
    if hasattr(cls, 'from_json'):
        return [cls.from_json(data) for data in array]
    else:
        return [cls(**data) for data in array]
