from functools import partial
from operator import add

from . import vector as vec
from ._utils import register_behavior, setup_field

__all__ = ['pair']

@register_behavior
@setup_field(add, 'charge')
class DiLepton(vec.DiLorentzVector):
    ...

pair = partial(vec.pair, name = 'DiLepton')