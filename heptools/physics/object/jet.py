from functools import partial

from ...aktools import add_arrays, get_field, or_arrays, to_tuple, where
from . import PhysicsObjectError
from . import vector as vec
from .utils import register_behavior, typestr

__all__ = ['pair', 'extend']

@register_behavior
class MultiJet(vec.MultiLorentzVector):
    ...

@register_behavior
class ExtendedJet(vec.MultiLorentzVector):
    def _unique_field(self, field = ()):
        constituents = self.constituents
        jets = to_tuple(constituents.Jet)
        p = add_arrays(*(get_field(jet, field) for jet in jets))
        others = set(constituents.fields) - {'Jet'}
        for other in others:
            objs = to_tuple(constituents[other])
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

def _type_check_extended_jet(ps):
    type_check = {'Jet', 'MultiJet', 'ExtendedJet'}
    for p in ps:
        if typestr(p) in type_check:
            return
    raise PhysicsObjectError(f'expected at least one of {type_check}, got [{", ".join(typestr(p) for p in ps)}]')

pair = partial(vec.pair, name = 'MultiJet', type_check = {'Jet', 'MultiJet'})
extend = partial(vec.pair, name = 'ExtendedJet', type_check = _type_check_extended_jet)