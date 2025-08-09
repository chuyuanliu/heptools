import awkward as ak
import numpy as np


def max_depth(array: ak.Array) -> int:
    if isinstance(array, np.ndarray):
        return 1
    return array.layout.minmax_depth[1]


def is_jagged(array: ak.Array) -> bool:
    return array.layout.minmax_depth[1] > 1


def is_array(array: ak.Array) -> bool:
    return len(array.fields) == 0
