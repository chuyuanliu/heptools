from __future__ import annotations

from typing import Callable, Literal

import awkward as ak
from awkward import Array
from coffea.nanoevents.methods import vector as vec

from ...aktools import partition
from ._utils import register_behavior, setup_lead_subl, setup_lorentz_vector

__all__ = ['pair', 'PairError']

class PairError(Exception):
    __module__ = Exception.__module__

@register_behavior(dependencies = vec.behavior)
@setup_lorentz_vector('_p')
@setup_lead_subl('mass', 'pt')
class DiLorentzVector(vec.PtEtaPhiMLorentzVector):
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

def _di_quad_check(*ps: Array, di = 'DiLorentzVector', quad = 'QuadLorentzVector'):
    di_fields = {'st', 'ht', 'dr'}
    return quad if all(di_fields <= set(p.fields) for p in ps) else di

def pair(*p: Array, mode: Literal['single', 'cartesian', 'combination'] = 'single', combinations: int = 1, name: str|Callable[[Array, Array], str] = _di_quad_check, behavior = None) -> Array:
    def check(expected: int):
        if len(p) != expected:
            raise PairError(f'expected {expected} arrays for {mode} mode, got {len(p)}')
    if isinstance(name, Callable):
        name = name(*p)
    if mode == 'single':
        check(2)
        return ak.zip({'_p1': p[0], '_p2': p[1]}, with_name = name, behavior = behavior)
    elif mode == 'cartesian':
        check(2)
        return ak.cartesian({'_p1': p[0], '_p2': p[1]}, with_name = name, behavior = behavior)
    elif mode == 'combination':
        check(1)
        if combinations == 1:
            return ak.combinations(p[0], 2, fields = ['_p1', '_p2'], with_name = name, behavior = behavior)
        else:
            return pair(*partition(p[0], combinations, 2), mode = 'single', name = name, behavior = behavior)
    