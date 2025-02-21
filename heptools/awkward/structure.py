import awkward as ak


def is_jagged(array: ak.Array) -> bool:
    return array.layout.minmax_depth[1] > 1


def is_array(array: ak.Array) -> bool:
    return len(array.fields) == 0
