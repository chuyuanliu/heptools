from __future__ import annotations

from typing import TYPE_CHECKING

import awkward as ak
from dask_awkward.lib.core import (
    is_typetracer,
    length_zero_array_or_identity,
    make_unknown_length,
)

if TYPE_CHECKING:
    import numpy.typing as npt

to_typetracer = make_unknown_length
touch_all = length_zero_array_or_identity


def len_maybe_typetracer(array: ak.Array) -> int:
    if is_typetracer(array):
        return 0
    return len(array)


def to_numpy_maybe_typetracer(
    array: ak.Array, allow_missing: bool = True
) -> npt.NDArray:
    if is_typetracer(array):
        array = ak.typetracer.length_zero_if_typetracer(array)
    return array.to_numpy(allow_missing=allow_missing)
