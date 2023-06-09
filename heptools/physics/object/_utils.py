import operator
from functools import partial

import awkward as ak

import heptools


def register_behavior(cls = None, dependencies: dict = None):
    if cls is None:
        return partial(register_behavior, dependencies = dependencies)
    _behavior = {}
    ak.mixin_class(_behavior)(cls)
    classname = cls.__name__
    _behavior[('__typestr__', classname)] = classname[0].lower() + classname[1:]
    _behavior[classname].__repr__ = lambda self: classname
    if dependencies:
        heptools.behavior |= dependencies
    heptools.behavior |= _behavior
    return cls

def setup_lorentz_vector(target: str):
    def _wrap(cls):
        def _get(self, name):
            return getattr(getattr(self, target), name)
        components = ['pt', 'eta', 'phi', 'mass']
        for k in components:
            setattr(cls, k, property(partial(_get, name = k)))
        cls.fields = cls.fields + components
        return cls
    return _wrap

def setup_lead_subl(*targets: str):
    def _wrap(cls):
        def _get(self, op, target):
            return ak.where(op(getattr(self._p1, target), getattr(self._p2, target)), self._p1, self._p2)
        fields = []
        for target in targets:
            for k, op in [('lead', operator.ge), ('subl', operator.lt)]:
                field = f'{k}_{target}'
                setattr(cls, field, property(partial(_get, op = op, target = target)))
                fields += [field]
        cls.fields = cls.fields + fields
        return cls
    return _wrap