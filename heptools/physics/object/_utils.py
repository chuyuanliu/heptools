from functools import partial
from operator import ge, lt
from typing import Callable, Iterable, Literal

import awkward as ak

from ...aktools import FieldLike, cache_field, get_field, get_shape, partition
from ...typetools import accumulated_mro
from ...utils import arg_new


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

def setup_lorentz_vector(target: str):
    def _wrap(cls):
        def _get(self, name):
            return get_field(get_field(self, target), name)
        for k in ['pt', 'eta', 'phi', 'mass']:
            setattr(cls, k, property(partial(_get, name = k)))
        return cls
    return _wrap

def setup_lead_subl(*targets: str):
    def _wrap(cls):
        def _get(self, op, target):
            return ak.where(op(get_field(self.obj1, target), get_field(self.obj2, target)), self.obj1, self.obj2)
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
    cache_field: list[FieldLike] = []
    type_check: set[str] | Callable[[Iterable[ak.Array]], None] = None

    @classmethod
    def pair(cls,
             *ps: ak.Array,
             mode: Literal['single', 'cartesian', 'combination'] = 'single',
             combinations: int = 1,
             cache: list[str] = ...) -> ak.Array:
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
                paired = ak.zip({'obj1': ps[0], 'obj2': ps[1]}, with_name = cls.name)
            case 'cartesian':
                check(2)
                paired = ak.cartesian({'obj1': ps[0], 'obj2': ps[1]}, with_name = cls.name)
            case 'combination':
                check(1)
                if combinations == 1:
                    paired = ak.combinations(ps[0], 2, fields = ['obj1', 'obj2'], with_name = cls.name)
                else:
                    paired = cls.pair(*partition(ps[0], combinations, 2), mode = 'single')
            case _:
                raise PhysicsObjectError(f'invalid mode "{mode}"')


        cache = arg_new(cache, list, lambda: accumulated_mro(cls, 'cache_field', reverse = True))
        for field in cache:
            cache_field(paired, field)
        return paired