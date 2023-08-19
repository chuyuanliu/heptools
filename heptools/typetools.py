from types import UnionType
from typing import Any, Iterable, Union, get_args, get_origin

__all__ = ['check_type', 'type_name']

# TODO check for list[], dict[], set[], tuple[], Literal[], __genericguard__, instance of object
def check_type(__obj, __class_or_tuple) -> bool:
    if __class_or_tuple is Any:
        return True
    origin = get_origin(__class_or_tuple)
    if origin is type:
        if not isinstance(__obj, type):
            return False
        return issubclass(__obj, get_args(__class_or_tuple))
    else:
        try:
            return isinstance(__obj, __class_or_tuple)
        except TypeError:
            return isinstance(__obj, origin)

def type_name(__type) -> str:
    if isinstance(__type, Iterable):
        return f'({", ".join(type_name(i) for i in __type)})'
    origin = get_origin(__type)
    args = get_args(__type)
    is_callable = len(args) == 2 and isinstance(args[0], list)
    args = [type_name(arg) for arg in args]
    if not origin and not args:
        if __type is Any:
            return 'Any'
        if __type is Ellipsis:
            return '...'
        if __type is None or __type is type(None):
            return 'None'
        return __type.__name__
    if is_callable:
        return f'{args[0]} -> {args[1]}'
    if origin is UnionType or origin is Union:
        return ' | '.join(args)
    if origin is type:
        if len(args) != 1:
            raise TypeError(f'invalid type <{__type}>')
        return f'<{args[0]}>'
    if args:
        args = f'[{", ".join(args)}]'
    return f'{type_name(origin)}{args}'