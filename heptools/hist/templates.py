from __future__ import annotations

from typing import Iterable

import awkward as ak
import numpy as np
from hist.axis import AxesMixin

from ..utils import astuple
from .hist import (Collection, FieldLike, Label, LabelLike, Template,
                   _default_field)


class Systematic(Template):
    def __init__(self, name: str, systs: Iterable[LabelLike], *axes: AxesMixin | tuple, weight: FieldLike = 'weight', **fill_args: FieldLike):
        super().__init__((name, ''), (), **fill_args)
        weight = astuple(weight)
        if len(axes) == 0:
            axes = Collection.current.duplicate_axes(name)
        for _var in systs:
            _var = Label(_var)
            self._name.display = f'({_var.display})'
            self._add(_var.code, *axes, weight = weight + _default_field(_var.code))
        self._name.display = ''


_H = Template._Hist

class Fourvector(Template):
    n       = _H((0, 20, ('n', 'Number')), n = ak.num)
    pt      = _H((100, 0, 500, ('pt', R'$p_{\mathrm{T}}$ [GeV]')))
    mass    = _H((100, 0, 500, ('mass', R'Mass [GeV]')))
    eta     = _H((100, -5, 5, ('eta', R'$\eta$')))
    phi     = _H((60, -np.pi, np.pi, ('phi', R'$\phi$')))
    pz      = _H((150, 0, 1500, ('pz', R'$p_{\mathrm{z}}$ [GeV]')))
    energy  = _H((150, 0, 1500, ('energy', R'Energy [GeV]')))

class Jet(Fourvector):
    deepjet_b   = _H((100, 0, 1, ('btagDeepFlavB', 'DeepJet $b$')))
    deepjet_c   = _H((100, 0, 1, ('btagDeepFlavCvL', 'DeepJet $c$ vs $uds+g$')),
                     (100, 0, 1, ('btagDeepFlavCvB', 'DeepJet $c$ vs $b$')))
    pileup_id   = _H(([0b000, 0b100, 0b110, 0b111], ('puId', 'Pileup ID')))
    jet_id      = _H(([0b000, 0b010, 0b110], ('jetId', 'Jet ID')))

class Lepton(Fourvector):
    charge  = _H((-2, 3, ('charge', 'Charge')))

class Muon(Lepton):
    ...

class Electron(Lepton):
    ...


class DiFourvector(Fourvector):
    dr      = _H((100, 0, 4, ('dr', R'$\Delta R$')))
    ht      = _H((100, 0, 1000, ('ht', R'$H_{\mathrm{T}}$ [GeV]')))

class DiLepton(DiFourvector, Lepton):
    ...