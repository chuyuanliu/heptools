from __future__ import annotations

from typing import TYPE_CHECKING

import awkward as ak
from dask_awkward.lib.core import (
    is_typetracer,
    make_unknown_length,
)

if TYPE_CHECKING:
    import numpy.typing as npt

to_typetracer = make_unknown_length


def len_may_typetracer(array: ak.Array) -> int:
    if is_typetracer(array):
        return 0
    return len(array)


def to_numpy_may_typetracer(array: ak.Array, allow_missing: bool = True) -> npt.NDArray:
    if is_typetracer(array):
        array = ak.typetracer.length_zero_if_typetracer(array)
    return array.to_numpy(allow_missing=allow_missing)
