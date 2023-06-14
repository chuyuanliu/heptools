from functools import partial

from coffea.nanoevents.methods import nanoaod

from . import vector as vec
from ._utils import register_behavior

__all__ = ['pair']

@register_behavior(dependencies = nanoaod.behavior)
class Dijet(vec.DiLorentzVector):
    ...

@register_behavior
class Quadjet(vec.QuadLorentzVector, Dijet):
    ...

_multiple_jet_check = partial(vec._di_quad_check, di = 'Dijet', quad = 'Quadjet')

pair = partial(vec.pair, name = _multiple_jet_check)