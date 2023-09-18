import awkward as ak

from ...aktools import get_shape, has_record
from ._patch import patch_coffea_nanoevent
from ._utils import PhysicsObjectError
from .jet import Jet
from .lepton import Lepton
from .muon import Muon
from .vector import LorentzVector

__all__ = ['with_index', 'PhysicsObjectError',
           'LorentzVector', 'Jet', 'Lepton', 'Muon']

def with_index(data: ak.Array, index: str = None):
    if index is None:
        name = get_shape(data)[-1]
        name = name[0].lower() + name[1:]
        index = f'{name}Idx'
    data = data[:]
    if not has_record(data, index):
        data[index] = ak.local_index(data, axis = 1)
    return data

patch_coffea_nanoevent()