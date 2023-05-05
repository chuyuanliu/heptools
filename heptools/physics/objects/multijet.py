from functools import partial

from coffea.nanoevents.methods import nanoaod

from . import multivector as mp
from ._utils import register_behavior

__all__ = ['pair', 'pair_all']

@register_behavior(dependencies = nanoaod.behavior)
class Dijet(mp.DiLorentzVector):
    ...

@register_behavior
class Quadjet(mp.QuadLorentzVector, Dijet):
    ...

_multiple_jet_check = partial(mp._multiple_lorentz_vector_check, di = 'Dijet', quad = 'Quadjet')

pair_all = partial(mp.pair_all, name = _multiple_jet_check)
pair = partial(mp.pair, name = _multiple_jet_check)