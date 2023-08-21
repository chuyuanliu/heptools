from __future__ import annotations

import operator
from functools import reduce
from typing import Iterable

from ..aktools import AnyArray
from ..utils import Eval
from .container import PartialSet, Tree

__all__ = ['Selection']

class Selection:
    def __init__(self):
        self.filters = Tree[PartialSet]()

    def add(self, selection: str, value: bool | Iterable[bool], *indices: AnyArray, default = False):
        self.filters |= Tree().from_dict({selection: PartialSet(value, *indices, default = default)})
        return self

    def __add__(self, other: Selection) -> Selection:
        if isinstance(other, Selection):
            new = Selection()
            new.filters = self.filters + other.filters
            return new
        return NotImplemented

    def __getitem__(self, selection: str) -> PartialSet:
        if selection in self.filters:
            return self.filters[selection]
        elif selection == '':
            return reduce(operator.and_, self.filters.values())
        else:
            return Eval(self.filters)[selection]

    def __call__(self, *indices: AnyArray, selection: str = ''):
        return self[selection](*indices)