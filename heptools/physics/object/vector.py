from __future__ import annotations

from collections import defaultdict
from operator import add
from typing import Callable

import awkward as ak
import numpy as np
from coffea.nanoevents.methods import vector as vec

from ...aktools import FieldLike, get_field, get_shape
from ...hist import H, Template
from ._utils import (Pair, register_behavior, setup_field, setup_lead_subl,
                     setup_lorentz_vector)


@register_behavior
@setup_lorentz_vector('p4vec')
@setup_lead_subl('pt', 'st', 'ht')
@setup_field(add, 'p4vec', 'st')
class DiLorentzVector(vec.PtEtaPhiMLorentzVector):
    def cumulate(self, op: Callable[[ak.Array, ak.Array], ak.Array], target: FieldLike):
        return op(get_field(self.obj1, target), get_field(self.obj2, target))

    @property
    def constituents(self):
        ps = defaultdict(list)
        for p in (self.obj1, self.obj2):
            try:
                constituents = p.constituents
                for k in constituents.fields:
                    ps[k].append(constituents[k])
            except:
                ps[get_shape(p)[-1]].append(ak.unflatten(p, bool(len(p)), axis = len(get_shape(p)) - 2))
        for k, v in ps.items():
            ps[k] = ak.concatenate(v, axis = len(get_shape((v[0]))) - 2)
        return ak.Array(ps, behavior = self.behavior)

    @property
    def dr(self):
        return self.obj1.delta_r(self.obj2)

    @property
    def dphi(self):
        return self.obj1.delta_phi(self.obj2)


class _PairLorentzVector(Pair):
    name = 'DiLorentzVector'
    cache_field = ['p4vec', 'pt', 'eta', 'phi', 'mass']


class _PlotLorentzVector(Template):
    n       = H((0, 20, ('n', 'Number')), n = ak.num)
    pt      = H((100, 0, 500, ('pt', R'$p_{\mathrm{T}}$ [GeV]')))
    mass    = H((100, 0, 500, ('mass', R'Mass [GeV]')))
    eta     = H((100, -5, 5, ('eta', R'$\eta$')))
    phi     = H((60, -np.pi, np.pi, ('phi', R'$\phi$')))
    pz      = H((100, -1000, 1000, ('pz', R'$p_{\mathrm{z}}$ [GeV]')))
    energy  = H((150, 0, 1500, ('energy', R'Energy [GeV]')))

class _PlotDiLorentzVector(_PlotLorentzVector):
    dr      = H((100, 0, 5, ('dr', R'$\Delta R$')))
    dphi    = H((60, -np.pi, np.pi, ('dphi', R'$\Delta\phi$')))
    ht      = H((100, 0, 1000, ('ht', R'$H_{\mathrm{T}}$ [GeV]')))


class LorentzVector:
    pair        = _PairLorentzVector.pair
    plot        = _PlotLorentzVector
    plot_pair   = _PlotDiLorentzVector