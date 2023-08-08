import awkward as ak

from ...aktools import get_dimension

__all__ = ['PhysicsObjectError', 'select']

class PhysicsObjectError(Exception):
    __module__ = Exception.__module__

def select(data: ak.Array, condition: ak.Array, add_index = False):
    selected = data[condition]
    if add_index:
        axis = get_dimension(condition) - 1
        index = ak.local_index(data, axis = axis)
        selected['index'] = index[condition]
    return selected