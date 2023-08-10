from __future__ import annotations

import operator
from functools import reduce
from typing import Iterable

from coffea.processor.accumulator import accumulate

from ..aktools import AnyArray
from ..utils import Eval
from .container import PartialSet

__all__ = ['Selection']

class Selection:
    def __init__(self, **filters: PartialSet):
        self.filters: dict[str, PartialSet] = filters

    def add(self, selection: str, value: bool | Iterable[bool], *indices: AnyArray):
        value = PartialSet(value, *indices)
        self.filters = accumulate((self.filters, {selection: value}))
        return self

    def __add__(self, other: Selection) -> Selection:
        if isinstance(other, Selection):
            return Selection(**accumulate((self.filters, other.filters)))
        else:
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