from functools import partial

from coffea.nanoevents.methods import nanoaod

from . import vector as vec
from ._utils import register_behavior

__all__ = ['pair']

@register_behavior(dependencies = nanoaod.behavior)
class MultiJet(vec.MultiLorentzVector):
    ...

pair = partial(vec.pair, name = 'MultiJet')