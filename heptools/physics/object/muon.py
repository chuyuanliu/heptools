from functools import partial

from . import lepton as lep
from .utils import register_behavior

__all__ = ['pair']

@register_behavior
class DiMuon(lep.DiLepton):
    ...

pair = partial(lep.pair, name = 'DiMuon', type_check = {'Muon', 'DiMuon'})