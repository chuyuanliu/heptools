import awkward as ak

from ...aktools import has_record
from ._utils import PhysicsObjectError, typestr
from .jet import Jet
from .lepton import Lepton
from .muon import Muon
from .vector import LorentzVector

__all__ = ['with_index', 'PhysicsObjectError',
           'LorentzVector', 'Jet', 'Lepton', 'Muon']

def with_index(data: ak.Array, index: str = None):
    if index is None:
        index = f'{typestr(data, "camelCase")}Idx'
    data = data[:]
    if not has_record(data, index):
        data[index] = ak.local_index(data, axis = 1)
    return data