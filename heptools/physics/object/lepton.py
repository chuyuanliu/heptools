from operator import add

from ...hist import H
from ._utils import Pair, register_behavior, setup_field
from .vector import DiLorentzVector, _Plot_DiLorentzVector, _Plot_LorentzVector


@register_behavior
@setup_field(add, 'charge')
class DiLepton(DiLorentzVector):
    ...


class _Pair_Lepton(Pair):
    name = 'DiLepton'


class _Plot_Common:
    charge = H((-2, 3, ('charge', 'Charge')))

class _Plot_Lepton(_Plot_Common, _Plot_LorentzVector):
    ...

class _Plot_DiLepton(_Plot_Common, _Plot_DiLorentzVector):
    ...


class Lepton:
    pair        = _Pair_Lepton.pair
    plot        = _Plot_Lepton
    plot_pair   = _Plot_DiLepton