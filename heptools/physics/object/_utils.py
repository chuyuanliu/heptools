from functools import partial
from operator import ge, lt
from typing import Callable, Iterable, Literal

import awkward as ak

import heptools

from ...aktools import partition


class PhysicsObjectError(Exception):
    __module__ = Exception.__module__

def typestr(array: ak.Array):
    name = str(ak.type(array)).split(' * ')[-1]
    return name[0].capitalize() + name[1:]

def register_behavior(cls = None, dependencies: dict = None):
    if cls is None:
        return partial(register_behavior, dependencies = dependencies)
    _behavior = {}
    ak.mixin_class(_behavior)(cls)
    classname = cls.__name__
    _behavior[('__typestr__', classname)] = classname
    _behavior[classname].__repr__ = lambda self: classname
    if dependencies:
        heptools.behavior |= dependencies
    heptools.behavior |= _behavior
    return cls

def setup_lorentz_vector(target: str):
    def _wrap(cls):
        def _get(self, name):
            return getattr(getattr(self, target), name)
        fields = ['pt', 'eta', 'phi', 'mass']
        for k in fields:
            setattr(cls, k, property(partial(_get, name = k)))
        cls.fields = cls.fields + fields
        return cls
    return _wrap

def setup_lead_subl(*targets: str):
    def _wrap(cls):
        def _get(self, op, target):
            return ak.where(op(getattr(self._p1, target), getattr(self._p2, target)), self._p1, self._p2)
        fields = []
        for target in targets:
            for k, op in [('lead', ge), ('subl', lt)]:
                field = f'{k}_{target}'
                setattr(cls, field, property(partial(_get, op = op, target = target)))
                fields += [field]
        cls.fields = cls.fields + fields
        return cls
    return _wrap

def setup_field(op: Callable[[ak.Array, ak.Array], ak.Array], *targets: str):
    def _wrap(cls):
        def _get(self, target):
            return op(getattr(self._p1, target), getattr(self._p2, target))
        for target in targets:
            setattr(cls, target, property(partial(_get, target = target)))
        cls.fields = cls.fields + [*targets]
        return cls
    return _wrap

class Pair:
    name: str = None
    type_check: set[str] | Callable[[Iterable[ak.Array]], None] = None

    @classmethod
    def pair(cls,
             *ps: ak.Array,
             mode: Literal['single', 'cartesian', 'combination'] = 'single',
             combinations: int = 1) -> ak.Array:
        if isinstance(cls.type_check, set):
            for p in ps:
                if typestr(p) not in cls.type_check:
                    raise PhysicsObjectError(f'expected {cls.type_check} (got <{typestr(p)}>)')
        elif isinstance(cls.type_check, Callable):
            cls.type_check(ps)
        def check(length: int):
            if len(ps) != length:
                raise PhysicsObjectError(f'expected {length} arrays for {mode} mode (got {len(ps)})')
        if mode == 'single':
            check(2)
            return ak.zip({'_p1': ps[0], '_p2': ps[1]}, with_name = cls.name)
        elif mode == 'cartesian':
            check(2)
            return ak.cartesian({'_p1': ps[0], '_p2': ps[1]}, with_name = cls.name)
        elif mode == 'combination':
            check(1)
            if combinations == 1:
                return ak.combinations(ps[0], 2, fields = ['_p1', '_p2'], with_name = cls.name)
            else:
                return cls.pair(*partition(ps[0], combinations, 2), mode = 'single')
        else:
            raise PhysicsObjectError(f'invalid mode "{mode}"')