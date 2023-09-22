from ._utils import register_behavior
from .lepton import DiLepton, _PairLepton, _PlotDiLepton, _PlotLepton


@register_behavior
class DiMuon(DiLepton):
    ...


class _PairMuon(_PairLepton):
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