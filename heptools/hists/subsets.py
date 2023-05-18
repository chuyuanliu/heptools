from __future__ import annotations

from typing import Iterable

import awkward as ak
import numpy as np
from hist.axis import AxesMixin

from .._utils import astuple
from .hists import FieldLike, Label, LabelLike, Set, Subset, _default_field


class Fourvector(Subset):
    def __init__(self, name: LabelLike, fill: FieldLike = None, pt = (100, 0, 500), mass = (100, 0, 500), pz = (150, 0, 1500), energy = (100, 0, 500), count = False, **fill_args: FieldLike):
        super().__init__(name, fill, **fill_args)
        if count:
            self.add('n', (1, 20, ('n', 'Number')), n = lambda x: ak.num(x[self._data]))
        self.add('pt'     , (               *pt,     ('pt', R'$p_{\mathrm{T}}$ [GeV]')))
        self.add('mass'   , (             *mass,   ('mass', R'Mass [GeV]')))
        self.add('eta'    , (100,     -5,     5,    ('eta', R'$\eta$')))
        self.add('phi'    , ( 60, -np.pi, np.pi,    ('phi', R'$\phi$')))
        self.add('pz'     , (               *pz,     ('pz', R'$p_{\mathrm{z}}$ [GeV]')))
        self.add('energy' , (           *energy, ('energy', R'Energy [GeV]')))

class DiFourvector(Fourvector):
    def __init__(self, name: LabelLike, fill: FieldLike = None, pt = (100, 0, 500), mass = (100, 0, 500), pz = (150, 0, 1500), energy = (100, 0, 500), dr = (100, 0, 4), count = False, **fill_args: FieldLike):
        super().__init__(name, fill, pt, mass, pz, energy, count, **fill_args)
        self.add('dr' , (*dr, ('dr', R'$\Delta R(p_1,p_2)$')))

class Systematic(Subset):
    def __init__(self, name: str, systs: Iterable[LabelLike], *axes: AxesMixin | tuple[int, float, float, LabelLike], weight: FieldLike = 'weight', **fill_args: FieldLike):
        super().__init__((name, ''), (), **fill_args)
        weight = astuple(weight)
        if len(axes) == 0:
            axes = Set.current.duplicate_axes(name)
        for _var in systs:
            _var = Label(_var)
            self._name.display = f'({_var.display})'
            self.add(_var.code, *axes, weight = weight + _default_field(_var.code))
        self._name.display = ''