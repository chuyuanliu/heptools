from __future__ import annotations

from collections import defaultdict
from typing import Callable, Iterable, Literal

import awkward as ak
from awkward import Array
from coffea.nanoevents.methods import vector as vec

from ...aktools import add_arrays, get_dimension, partition
from . import PhysicsObjectError
from .utils import (register_behavior, setup_lead_subl, setup_lorentz_vector,
                    typestr)

__all__ = ['pair']

@register_behavior
@setup_lorentz_vector('_p')
@setup_lead_subl('mass', 'pt')
class DiLorentzVector(vec.PtEtaPhiMLorentzVector):
    fields = ['st', 'ht', 'dr', 'constituents']

    @property
    def constituents(self):
        '''all constituents'''
        ps = defaultdict(list)
        for p in (self._p1, self._p2):
            if 'constituents' in p.fields:
                constituents = p.constituents
                for k in constituents.fields:
                    ps[k].append(constituents[k])
            else:
                ps[typestr(p)].append(ak.unflatten(p, 1, axis = get_dimension(p) - 1))
        for k, v in ps.items():
            ps[k] = ak.concatenate(v, axis = get_dimension(v[0]) - 1)
        return ak.Array(ps, behavior = self.behavior)

    @property
    def _p(self):
        '''four-momentum'''
        return self._p1 + self._p2

    @property
    def st(self):
        '''scalar sum of `pt` (ATLAS)'''
        return add_arrays(*(p.st if 'st' in p.fields else p.pt for p in (self._p1, self._p2)))
    @property
    def ht(self):
        '''scalar sum of `pt` (CMS)'''
        return self.st

    @property
    def dr(self):
        '''delta R'''
        return self._p1.delta_r(self._p2)

def pair(*ps: Array, mode: Literal['single', 'cartesian', 'combination'] = 'single', combinations: int = 1, name: str = 'DiLorentzVector', behavior = None, type_check: set[str] | Callable[[Iterable[Array]], None] = None) -> Array:
    if isinstance(type_check, set):
        for p in ps:
            if typestr(p) not in type_check:
                raise PhysicsObjectError(f'expected {type_check}, got <{typestr(p)}>')
    elif isinstance(type_check, Callable):
        type_check(ps)
    def check(length: int):
        if len(ps) != length:
            raise PhysicsObjectError(f'expected {length} arrays for {mode} mode, got {len(ps)}')
    if mode == 'single':
        check(2)
        return ak.zip({'_p1': ps[0], '_p2': ps[1]}, with_name = name, behavior = behavior)
    elif mode == 'cartesian':
        check(2)
        return ak.cartesian({'_p1': ps[0], '_p2': ps[1]}, with_name = name, behavior = behavior)
    elif mode == 'combination':
        check(1)
        if combinations == 1:
            return ak.combinations(ps[0], 2, fields = ['_p1', '_p2'], with_name = name, behavior = behavior)
        else:
            return pair(*partition(ps[0], combinations, 2), mode = 'single', name = name, behavior = behavior)
    else:
        raise PhysicsObjectError(f'invalid mode "{mode}"')