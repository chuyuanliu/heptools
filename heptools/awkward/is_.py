import awkward as ak


def jagged(array: ak.Array) -> bool:
    return array.layout.minmax_depth[1] > 1
