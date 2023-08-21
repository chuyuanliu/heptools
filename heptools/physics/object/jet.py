from functools import partial

from ...aktools import (FieldLike, add_arrays, foreach, get_field, or_arrays,
                        where)
from . import PhysicsObjectError
from . import vector as vec
from .utils import register_behavior, typestr

__all__ = ['pair', 'extend']

@register_behavior
class DiJet(vec.DiLorentzVector):
    ...

@register_behavior
class ExtendedJet(vec.DiLorentzVector):
    def _unique_field(self, field: FieldLike = ()):
        constituents = self.constituents
        jets = foreach(constituents.Jet)
        p = add_arrays(*(get_field(jet, field) for jet in jets))
        others = set(constituents.fields) - {'Jet'}
        for other in others:
            objs = foreach(constituents[other])
            for obj in objs:
                p = where(p + get_field(obj, field),
                          (or_arrays(*(obj.jetIdx == jet.index for jet in jets)), p))
        return p

    @property
    def _p(self):
        return self._unique_field()

    @property
    def st(self):
        return self._unique_field('pt')

    # TODO count

def _type_check_extended_jet(ps):
    type_check = {'Jet', 'DiJet', 'ExtendedJet'}
    for p in ps:
        if typestr(p) in type_check:
            return
    raise PhysicsObjectError(f'expected at least one of {type_check} (got [{", ".join(typestr(p) for p in ps)}])')

pair = partial(vec.pair, name = 'DiJet', type_check = {'Jet', 'DiJet'})
extend = partial(vec.pair, name = 'ExtendedJet', type_check = _type_check_extended_jet)