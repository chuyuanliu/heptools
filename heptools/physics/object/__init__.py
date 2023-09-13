import awkward as ak

from ...aktools import get_dimension
from ._utils import PhysicsObjectError
from .jet import Jet
from .lepton import Lepton
from .muon import Muon
from .vector import LorentzVector

__all__ = ['PhysicsObjectError', 'select',
           'LorentzVector', 'Jet', 'Lepton', 'Muon']

def select(data: ak.Array, condition: ak.Array, add_index = False):
    selected = data[condition]
    if add_index:
        axis = get_dimension(condition) - 1
        index = ak.local_index(data, axis = axis)
        selected['index'] = index[condition]
    return selected