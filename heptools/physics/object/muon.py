from ._utils import register_behavior
from .lepton import DiLepton, _Pair_Lepton, _Plot_DiLepton, _Plot_Lepton


@register_behavior
class DiMuon(DiLepton):
    ...

class _Pair_Muon(_Pair_Lepton):
    name = 'DiMuon'
    type_check = {'Muon', 'DiMuon'}

class _Plot_Muon(_Plot_Lepton):
    ...

class Muon:
    pair        = _Pair_Muon.create
    plot        = _Plot_Muon
    plot_pair   = _Plot_DiLepton