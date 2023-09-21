from functools import partial
from operator import ge, lt
from typing import Callable, Iterable, Literal

import awkward as ak

from ...aktools import get_shape, partition


class PhysicsObjectError(Exception):
    __module__ = Exception.__module__

def register_behavior(cls = None, dependencies: dict = None):
    from ... import behavior
    if cls is None:
        return partial(register_behavior, dependencies = dependencies)
    ak.mixin_class(behavior)(cls)
    classname = cls.__name__
    behavior[('__typestr__', classname)] = classname
    behavior[classname].__repr__ = lambda _: classname
    if dependencies:
        behavior |= dependencies
    return cls

# TODO cache fields
def setup_lorentz_vector(target: str):
    def _wrap(cls):
        def _get(self, name):
            return getattr(getattr(self, target), name)
        for k in ['pt', 'eta', 'phi', 'mass']:
            setattr(cls, k, property(partial(_get, name = k)))
        return cls
    return _wrap

def setup_lead_subl(*targets: str):
    def _wrap(cls):
        def _get(self, op, target):
            return ak.where(op(getattr(self._p1, target), getattr(self._p2, target)), self._p1, self._p2)
        for target in targets:
            for k, op in [('lead', ge), ('subl', lt)]:
                field = f'{k}_{target}'
                setattr(cls, field, property(partial(_get, op = op, target = target)))
        return cls
    return _wrap

def setup_field(op: Callable[[ak.Array, ak.Array], ak.Array], *targets: str):
    def _wrap(cls):
        def _get(self, target):
            return self.cumulate(op, target)
        for target in targets:
            setattr(cls, target, property(partial(_get, target = target)))
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
                if get_shape(p)[-1] not in cls.type_check:
                    raise PhysicsObjectError(f"expected {cls.type_check} (got '{get_shape(p)[-1]}')")
        elif isinstance(cls.type_check, Callable):
            cls.type_check(ps)
        def check(length: int):
            if len(ps) != length:
                raise PhysicsObjectError(f'expected {length} arrays for {mode} mode (got {len(ps)})')
        match mode:
            case 'single':
                check(2)
                return ak.zip({'_p1': ps[0], '_p2': ps[1]}, with_name = cls.name)
            case 'cartesian':
                check(2)
                return ak.cartesian({'_p1': ps[0], '_p2': ps[1]}, with_name = cls.name)
            case 'combination':
                check(1)
                if combinations == 1:
                    return ak.combinations(ps[0], 2, fields = ['_p1', '_p2'], with_name = cls.name)
                else:
                    return cls.pair(*partition(ps[0], combinations, 2), mode = 'single')
            case _:
                raise PhysicsObjectError(f'invalid mode "{mode}"')