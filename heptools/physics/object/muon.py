from ._utils import Pair, register_behavior
from .lepton import DiLepton, _PlotDiLepton, _PlotLepton


@register_behavior
class DiMuon(DiLepton):
    ...


class _PairMuon(Pair):
    name = 'DiMuon'
    type_check = {'Muon', 'DiMuon'}


class _PlotCommon:
    ...

class _PlotMuon(_PlotCommon, _PlotLepton):
    ...

class _PlotDiMuon(_PlotCommon, _PlotDiLepton):
    ...


class Muon:
    pair        = _PairMuon.pair
    plot        = _PlotMuon
    plot_pair   = _PlotDiMuon