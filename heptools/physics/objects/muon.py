from functools import partial

from coffea.nanoevents.methods import nanoaod

from . import lepton as lep, vector as vec
from ._utils import register_behavior

__all__ = ['pair']

@register_behavior
class DiMuon(lep.DiLepton):
    ...

pair = partial(vec.pair, name = 'DiMuon')