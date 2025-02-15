import awkward as ak


def is_jagged(array: ak.Array) -> bool:
    return array.layout.minmax_depth[1] > 1
