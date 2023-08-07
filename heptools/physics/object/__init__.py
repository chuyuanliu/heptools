import awkward as ak


def select(data: ak.Array, condition: ak.Array, add_index = False):
    selected = data[condition]
    if add_index:
        axis = condition.layout.minmax_depth[0] - 1
        index = ak.local_index(data, axis = axis)
        selected['index'] = index[condition]
    return selected