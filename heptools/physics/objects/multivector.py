from __future__ import annotations

from typing import Callable

import awkward as ak
from awkward import Array
from coffea.nanoevents.methods import vector

from ...combination import partition
from ._utils import register_behavior, setup_lead_subl, setup_lorentz_vector

__all__ = ['pair', 'pair_all']

@register_behavior(dependencies = vector.behavior)
@setup_lorentz_vector('_p')
@setup_lead_subl('mass', 'pt')
class DiLorentzVector(vector.PtEtaPhiMLorentzVector):
    fields = ['st', 'ht', 'dr']

    @property
    def _p(self):
        '''four-momentum'''
        return self._p1 + self._p2

    @property
    def st(self):
        '''scalar sum of `pt` (ATLAS)'''
        return self._p1.pt + self._p2.pt
    @property
    def ht(self):
        '''scalar sum of `pt` (CMS)'''
        return self.st
    @property
    def dr(self):
        '''delta R'''
        return self._p1.delta_r(self._p2)

@register_behavior
@setup_lead_subl('st', 'ht')
class QuadLorentzVector(DiLorentzVector):
    ...

def _multiple_lorentz_vector_check(*ps: Array, di = 'DiLorentzVector', quad = 'QuadLorentzVector'):
    di_fields = {'st', 'ht', 'dr'}
    return quad if all(di_fields <= set(p.fields) for p in ps) else di

def pair(p1: Array, p2: Array, name: str|Callable[[Array, Array], str] = _multiple_lorentz_vector_check, behavior = None) -> Array:
    if isinstance(name, Callable):
        name = name(p1, p2)
    return ak.zip({'_p1': p1, '_p2': p2}, with_name = name, behavior = behavior)

def pair_all(p: Array, pairs: int = 1, name: str|Callable[[Array], str] = _multiple_lorentz_vector_check, behavior = None) -> Array:
    if isinstance(name, Callable):
        name = name(p)
    if pairs == 1:
        return ak.combinations(p, 2, fields = ['_p1', '_p2'], with_name = name, behavior = behavior)
    else:
        return pair(*partition(p, pairs, 2), name = name, behavior = behavior)