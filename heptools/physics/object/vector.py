from __future__ import annotations

from collections import defaultdict
from operator import add

import awkward as ak
import numpy as np
from coffea.nanoevents.methods import vector as vec

from ...aktools import get_shape
from ...hist import H, Template
from ._utils import (Pair, register_behavior, setup_field, setup_lead_subl,
                     setup_lorentz_vector)


@register_behavior
@setup_lorentz_vector('p4vec')
@setup_lead_subl('pt', 'st', 'ht')
@setup_field(add, 'st')
class DiLorentzVector(vec.PtEtaPhiMLorentzVector):
    @property
    def constituents(self):
        ps = defaultdict(list)
        for p in (self._p1, self._p2):
            try:
                constituents = p.constituents
                for k in constituents.fields:
                    ps[k].append(constituents[k])
            except:
                ps[get_shape(p)[-1]].append(ak.unflatten(p, 1, axis = len(get_shape(p)) - 2))
        for k, v in ps.items():
            ps[k] = ak.concatenate(v, axis = len(get_shape((v[0]))) - 2)
        return ak.Array(ps, behavior = self.behavior)

    @property
    def p4vec(self):
        return self._p1 + self._p2

    @property
    def dr(self):
        return self._p1.delta_r(self._p2)


class _PairLorentzVector(Pair):
    name = 'DiLorentzVector'


class _PlotLorentzVector(Template):
    n       = H((0, 20, ('n', 'Number')), n = ak.num)
    pt      = H((100, 0, 500, ('pt', R'$p_{\mathrm{T}}$ [GeV]')))
    mass    = H((100, 0, 500, ('mass', R'Mass [GeV]')))
    eta     = H((100, -5, 5, ('eta', R'$\eta$')))
    phi     = H((60, -np.pi, np.pi, ('phi', R'$\phi$')))
    pz      = H((150, 0, 1500, ('pz', R'$p_{\mathrm{z}}$ [GeV]')))
    energy  = H((150, 0, 1500, ('energy', R'Energy [GeV]')))

class _PlotDiLorentzVector(_PlotLorentzVector):
    dr      = H((100, 0, 4, ('dr', R'$\Delta R$')))
    ht      = H((100, 0, 1000, ('ht', R'$H_{\mathrm{T}}$ [GeV]')))


class LorentzVector:
    pair        = _PairLorentzVector.pair
    plot        = _PlotLorentzVector
    plot_pair   = _PlotDiLorentzVector