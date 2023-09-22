from operator import add

from ...hist import H
from ._utils import register_behavior, setup_field
from .vector import (DiLorentzVector, _PairLorentzVector, _PlotDiLorentzVector,
                     _PlotLorentzVector)


@register_behavior
@setup_field(add, 'charge')
class DiLepton(DiLorentzVector):
    ...


class _PairLepton(_PairLorentzVector):
    name = 'DiLepton'


class _PlotCommon:
    charge = H((-2, 3, ('charge', 'Charge')))

class _PlotLepton(_PlotCommon, _PlotLorentzVector):
    ...

class _PlotDiLepton(_PlotCommon, _PlotDiLorentzVector):
    ...


class Lepton:
    pair        = _PairLepton.pair
    plot        = _PlotLepton
    plot_pair   = _PlotDiLepton