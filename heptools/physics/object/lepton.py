from operator import add

from ._utils import register_behavior, setup_field
from .vector import (DiLorentzVector, H, _Pair_LorentzVector,
                     _Plot_DiLorentzVector, _Plot_LorentzVector)


@register_behavior
@setup_field(add, 'charge')
class DiLepton(DiLorentzVector):
    ...

class _Pair_Lepton(_Pair_LorentzVector):
    name = 'DiLepton'

class _Plot_Lepton(_Plot_LorentzVector):
    charge = H((-2, 3, ('charge', 'Charge')))

class _Plot_DiLepton(_Plot_Lepton, _Plot_DiLorentzVector):
    ...

class Lepton:
    pair        = _Pair_Lepton.create
    plot        = _Plot_Lepton
    plot_pair   = _Plot_DiLepton