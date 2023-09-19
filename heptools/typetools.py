import json
from types import UnionType
from typing import Any, Generic, Iterable, Literal, Union, get_args, get_origin

from .utils import unique

__all__ = ['check_type', 'type_name']

class DefaultEncoder(json.JSONEncoder):
    def default(self, __obj):
        if '__json__' in dir(__obj):
            return __obj.__json__()
        return super().default(__obj)

def _type_hint_init(self):
    raise TypeError(f'<{self.__class__.__name__}> is for type hint only')
def type_hint_only(cls):
    cls.__init__ = _type_hint_init
    return cls

def alias(*methods: str):
    def wrapper(cls):
        for method in methods:
            if not hasattr(cls, method):
                raise TypeError(f'`{cls.__name__}.{method}()` is not defined')
            setattr(cls, f'__{method}__', getattr(cls, method))
        return cls
    return wrapper

def _expand_type(__class_or_tuple):
    origin = get_origin(__class_or_tuple)
    args = get_args(__class_or_tuple)
    generic = origin is not None and len(args) > 0
    if origin is None:
        origin = __class_or_tuple
    return origin, args, generic


def check_subclass(__derived, __base) -> bool:
    if __base is Any:
        return True
    if __derived is Any:
        return False

    origin_base, args_base, _ = _expand_type(__base)
    origin_derived, args_derived, _ = _expand_type(__derived)

    # Union
    union_base = origin_base is UnionType or origin_base is Union
    union_derived = origin_derived is UnionType or origin_derived is Union
    if union_base:
        if not union_derived:
            args_derived = (__derived,)
        return all(any(check_subclass(i, j) for j in args_base) for i in args_derived)
    elif union_derived:
        return all(check_subclass(i, __base) for i in args_derived)
    # Literal
    if origin_base is Literal and origin_derived is Literal:
        return all(i in args_base for i in args_derived)
    if origin_derived is Literal or origin_base is Literal:
        return False

    if not issubclass(origin_derived, origin_base):
        return False
    if len(args_base) > len(args_derived):
        return False
    for i in range(len(args_base)):
        if not check_subclass(args_derived[i], args_base[i]):
            return False
    return True

# TODO check Callable
def check_type(__obj, __type) -> bool:
    if __type is Any:
        return True
    if __type.__class__ is object:
        return __obj is __type

    origin, args, generic = _expand_type(__type)

    # Union
    if origin is UnionType or origin is Union:
        return any(check_type(__obj, i) for i in args)
    # Literal
    if origin is Literal:
        return __obj in args

    if issubclass(origin, Generic):
        if origin.__init__ is _type_hint_init:
            return check_type(__obj, args[0]) if generic else True

    if generic:
        if not isinstance(__obj, origin):
            return False
        if origin is type:
            return check_subclass(__obj, args[0])
        if origin is list or origin is set:
            return all(check_type(i, args[0]) for i in __obj)
        if origin is dict:
            if not all(check_type(i, args[0]) for i in __obj.keys()):
                return False
            if len(args) == 2:
                return all(check_type(i, args[1]) for i in __obj.values())
        if origin is tuple:
            if len(args) == 2 and args[1] is ...:
                return all(check_type(i, args[0]) for i in __obj)
            if len(args) != len(__obj):
                return False
            return all(check_type(__obj[i], args[i]) for i in range(len(args)))
        if '__typeguard__' in dir(origin):
            return origin.__typeguard__(__obj, *args)
        return True
    else:
        return isinstance(__obj, origin)

def type_name(origin) -> str:
    if isinstance(origin, Iterable):
        return f'({", ".join(type_name(i) for i in origin)})'
    origin, args, generic = _expand_type(origin)
    is_callable = len(args) == 2 and isinstance(args[0], list)
    args = unique(type_name(var) for var in args)
    if not generic:
        if origin is Any:
            return 'Any'
        if origin is Ellipsis:
            return '...'
        if origin is None or origin is type(None):
            return 'None'
        try:
            return origin.__name__
        except:
            return str(origin)
    if is_callable:
        return f'{args[0]} -> {args[1]}'
    if origin is UnionType or origin is Union:
        if 'None' in args:
            args.remove('None')
            return f'Optional[{" | ".join(args)}]'
        return ' | '.join(args)
    if origin is type:
        return f'<{args[0]}>'
    if args:
        args = f'[{", ".join(args)}]'
    return f'{type_name(origin)}{args}'