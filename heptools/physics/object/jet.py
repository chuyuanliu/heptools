from operator import add
from typing import Callable

import awkward as ak

from ...aktools import (FieldLike, foreach, get_field, get_shape, op_arrays,
                        or_arrays, where)
from ...hist import H
from ._utils import PhysicsObjectError, register_behavior
from .vector import (DiLorentzVector, _PairLorentzVector, _PlotDiLorentzVector,
                     _PlotLorentzVector)


@register_behavior
class DiJet(DiLorentzVector):
    ...

@register_behavior
class ExtendedJet(DiLorentzVector):
    def cumulate(self, op: Callable[[ak.Array, ak.Array], ak.Array], field: FieldLike):
        if len(self) > 0:
            constituents = self.constituents
            jets = foreach(constituents.Jet)
            p = op_arrays(*(get_field(jet, field) for jet in jets), op = op)
            others = set(constituents.fields) - {'Jet'}
            for other in others:
                objs = foreach(constituents[other])
                for obj in objs:
                    p = where(op(p, get_field(obj, field)),
                            (or_arrays(*(obj.jetIdx == jet.jetIdx for jet in jets)), p))
            return p
        else:
            return super().cumulate(op, field)

    @property
    def n_unique(self):
        return self.cumulate(add, ...)


class _PairJet(_PairLorentzVector):
    name = 'DiJet'
    type_check = {'Jet', 'DiJet'}

class _ExtendJet(_PairLorentzVector):
    name = 'ExtendedJet'
    @staticmethod
    def type_check(ps):
        type_check = {'Jet', 'DiJet', 'ExtendedJet'}
        for p in ps:
            if get_shape(p)[-1] in type_check:
                return
        raise PhysicsObjectError(f'expected at least one of {type_check} (got {set(get_shape(p)[-1] for p in ps)})')


class _PlotCommon:
    ...

class _PlotJet(_PlotCommon, _PlotLorentzVector):
    deepjet_b   = H((100, 0, 1, ('btagDeepFlavB', 'DeepJet $b$')))
    deepjet_c   = H((100, 0, 1, ('btagDeepFlavCvL', 'DeepJet $c$ vs $uds+g$')),
                    (100, 0, 1, ('btagDeepFlavCvB', 'DeepJet $c$ vs $b$')))
    id_pileup   = H(([0b000, 0b100, 0b110, 0b111], ('puId', 'Pileup ID')))
    id_jet      = H(([0b000, 0b010, 0b110], ('jetId', 'Jet ID')))

class _PlotDiJet(_PlotCommon, _PlotDiLorentzVector):
    ...


class Jet:
    pair        = _PairJet.pair
    extend      = _ExtendJet.pair
    plot        = _PlotJet
    plot_pair   = _PlotDiJet