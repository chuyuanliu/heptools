from __future__ import annotations

from collections import defaultdict

import awkward as ak
import numpy as np
from coffea.nanoevents.methods import vector as vec

from ...aktools import add_arrays, get_dimension
from ...hist import H, Template
from ._utils import (Pair, register_behavior, setup_lead_subl,
                     setup_lorentz_vector, typestr)


@register_behavior
@setup_lorentz_vector('_p')
@setup_lead_subl('mass', 'pt')
class DiLorentzVector(vec.PtEtaPhiMLorentzVector):
    fields = ['st', 'ht', 'dr', 'constituents']

    @property
    def constituents(self):
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
        return self._p1 + self._p2

    @property
    def st(self):
        return add_arrays(*(p.st if 'st' in p.fields else p.pt for p in (self._p1, self._p2)))
    @property
    def ht(self):
        return self.st

    @property
    def dr(self):
        return self._p1.delta_r(self._p2)


class _Pair_LorentzVector(Pair):
    name = 'DiLorentzVector'


class _Plot_LorentzVector(Template):
    n       = H((0, 20, ('n', 'Number')), n = ak.num)
    pt      = H((100, 0, 500, ('pt', R'$p_{\mathrm{T}}$ [GeV]')))
    mass    = H((100, 0, 500, ('mass', R'Mass [GeV]')))
    eta     = H((100, -5, 5, ('eta', R'$\eta$')))
    phi     = H((60, -np.pi, np.pi, ('phi', R'$\phi$')))
    pz      = H((150, 0, 1500, ('pz', R'$p_{\mathrm{z}}$ [GeV]')))
    energy  = H((150, 0, 1500, ('energy', R'Energy [GeV]')))

class _Plot_DiLorentzVector(_Plot_LorentzVector):
    dr      = H((100, 0, 4, ('dr', R'$\Delta R$')))
    ht      = H((100, 0, 1000, ('ht', R'$H_{\mathrm{T}}$ [GeV]')))


class LorentzVector:
    pair        = _Pair_LorentzVector.pair
    plot        = _Plot_LorentzVector
    plot_pair   = _Plot_DiLorentzVector