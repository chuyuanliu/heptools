from collections.abc import Callable
from functools import reduce
from inspect import getmro
from operator import add
from types import UnionType
from typing import (Annotated, Any, Literal, Protocol, TypeVar, Union,
                    get_args, get_origin, get_type_hints, runtime_checkable)

from .utils import count, unique

__all__ = ['TypeGuarded',
           'check_subclass', 'check_type', 'type_name',
           'reversed_mro']


@runtime_checkable
class TypeGuarded(Protocol):
    def __typeguard__(self, *args) -> bool:
        ...


def _expand_type(__class_or_tuple):
    origin = get_origin(__class_or_tuple)
    args = get_args(__class_or_tuple)
    generic = origin is not None and len(args) > 0
    if origin is None:
        origin = __class_or_tuple
    if isinstance(origin, TypeVar):
        origin = origin.__bound__ if origin.__bound__ is not None else Any
    return origin, args, generic


def check_subclass(__derived, __base) -> bool:
    # TODO Callable, TypedDict

    origin_base, args_base, _ = _expand_type(__base)
    origin_derived, args_derived, _ = _expand_type(__derived)

    # Any
    if origin_base is Any:
        return True
    if origin_derived is Any:
        return False

    # None, Ellipsis
    for type_ in (None, ...):
        match count([origin_derived, origin_base], type_):
            case 2:
                return True
            case 1:
                return False

    # Union
    union_base = origin_base is UnionType or origin_base is Union
    union_derived = origin_derived is UnionType or origin_derived is Union
    if union_base:
        if not union_derived:
            args_derived = (__derived,)
        return all(any(check_subclass(i, j) for j in args_base) for i in args_derived)
    elif union_derived:
        return all(check_subclass(i, __base) for i in args_derived)
    # Literal, Annotated, Callable
    for type_, func in (
        (Literal, lambda:
            all(i in args_base for i in args_derived)),
        (Annotated, lambda:
            args_base[1:] == args_derived[1:]
            and check_subclass(args_derived[0], args_base[0])),
        (Callable, lambda:
            True)  # FIXME compare args
    ):
        match count([origin_base, origin_derived], type_):
            case 2:
                return func()
            case 1:
                return False

    if not issubclass(origin_derived, origin_base):
        return False
    if len(args_base) > len(args_derived):
        return False
    for i in range(len(args_base)):
        if not check_subclass(args_derived[i], args_base[i]):
            return False
    return True


def check_type(__obj, __type) -> bool:
    # TODO Callable, TypedDict

    # object(), None, Ellipsis
    if __type.__class__ is object or __type is None or __type is ...:
        return __obj is __type

    origin, args, generic = _expand_type(__type)

    # Any
    if origin is Any:
        return True

    # Union
    if origin is UnionType or origin is Union:
        return any(check_type(__obj, i) for i in args)
    # Literal
    if origin is Literal:
        return __obj in args
    # Annotated
    if origin is Annotated:
        return check_type(__obj, args[0])

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
        if isinstance(origin, TypeGuarded):
            return origin.__typeguard__(__obj, *args)
        return True
    else:
        return isinstance(__obj, origin)


def type_name(__type) -> str:
    # TODO TypedDict
    if isinstance(__type, str):
        return __type
    if isinstance(__type, tuple | list):
        return f'({", ".join(type_name(i) for i in __type)})'

    origin, args, generic = _expand_type(__type)
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

    # Union
    if origin is UnionType or origin is Union:
        if 'None' in args:
            args.remove('None')
            return f'Optional[{" | ".join(args)}]'
        return ' | '.join(args)
    # Annotated
    if origin is Annotated:
        return f'[{", ".join(args[1:])}] {args[0]}'
    # Callable
    if origin is Callable:
        return f'{args[0]} -> {args[1]}'
    # type
    if origin is type:
        return f'<{args[0]}>'

    if args:
        args = f'[{", ".join(args)}]'
    return f'{type_name(origin)}{args}'


def get_partial_type_hints(cls, include_extras: bool = False):
    try:
        return get_type_hints(cls, include_extras=include_extras)
    except:
        if not hasattr(cls, '__annotations__'):
            return {}

        class dummy:
            __annotations__ = {}
        hints = {}
        for k, v in cls.__annotations__.items():
            dummy.__annotations__ = {k: v}
            try:
                hints[k] = get_type_hints(
                    dummy, include_extras=include_extras)[k]
            except:
                hints[k] = v
        return hints

# mro


def reversed_mro(cls, name: str):
    for base in getmro(cls)[::-1]:
        if name in vars(base):
            return base, vars(base)[name]
    raise AttributeError


def accumulated_mro(cls, name: str, reverse: bool = False, op: Callable[[Any, Any], Any] = add):
    return reduce(op,
                  (vars(base)[name]
                   for base in getmro(cls)[::-1 if reverse else 1]
                   if name in vars(base)))
