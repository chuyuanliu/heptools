from ._utils import Pair, register_behavior
from .lepton import DiLepton, _Plot_DiLepton, _Plot_Lepton


@register_behavior
class DiMuon(DiLepton):
    ...


class _Pair_Muon(Pair):
    name = 'DiMuon'
    type_check = {'Muon', 'DiMuon'}


class _Plot_Common:
    ...

class _Plot_Muon(_Plot_Common, _Plot_Lepton):
    ...

class _Plot_DiMuon(_Plot_Common, _Plot_DiLepton):
    ...


class Muon:
    pair        = _Pair_Muon.pair
    plot        = _Plot_Muon
    plot_pair   = _Plot_DiMuon