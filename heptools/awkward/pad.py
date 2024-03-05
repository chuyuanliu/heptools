from typing import Any

import awkward as ak
import numpy as np
import numpy.typing as npt

from .to import numpy as to_numpy


class selected:
    def __init__(
        self,
        padded_value: Any = 0,
        jagged_size: int = 0,
    ):
        self._value = padded_value
        self._size = jagged_size

    def _pad_regular(self, array: npt.NDArray, selection: npt.NDArray) -> npt.NDArray:
        padded = np.full(len(selection), self._value, dtype=array.dtype)
        padded[selection] = array
        return padded

    def _pad_jagged(self, array: npt.NDArray, count: npt.NDArray, selection: npt.NDArray) -> npt.NDArray:
        shape = (len(selection) - len(count)) * self._size + count.sum()
        padded_count = np.full(len(selection), self._size, dtype=count.dtype)
        padded_count[selection] = count
        padded = np.full(shape, self._value, dtype=array.dtype)
        padded[np.repeat(selection, padded_count)] = array
        return ak.unflatten(padded, padded_count)

    def _pad(self, array: npt.NDArray | tuple[npt.NDArray, npt.NDArray], selection: npt.NDArray) -> npt.NDArray:
        if isinstance(array, tuple):
            return self._pad_jagged(*array, selection)
        else:
            return self._pad_regular(array, selection)

    def __call__(
        self,
        array: ak.Array,
        selection: npt.ArrayLike
    ) -> ak.Array:
        selection = np.asarray(selection, dtype=bool)
        if ak.fields(array):
            padded = {
                name: self._pad(arr, selection)
                for name, arr in to_numpy(array).items()}
        else:
            padded = self._pad(ak.to_numpy(array), selection)
        return ak.Array(padded)
