from functools import partial

from . import lepton as lep
from .utils import register_behavior

__all__ = ['pair']

@register_behavior
class MultiMuon(lep.MultiLepton):
    ...

pair = partial(lep.pair, name = 'MultiMuon', type_check = {'Muon', 'MultiMuon'})