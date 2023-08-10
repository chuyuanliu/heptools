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
        self._filters: dict[str, PartialSet] = filters

    def add(self, selection: str, value: bool | Iterable[bool], *indices: AnyArray):
        value = PartialSet(value, *indices)
        self._filters = accumulate((self._filters, {selection: value}))
        return self

    def __add__(self, other: Selection) -> Selection:
        if isinstance(other, Selection):
            return Selection(**accumulate((self._filters, other._filters)))
        else:
            return NotImplemented

    def __getitem__(self, selection: str) -> PartialSet:
        if selection in self._filters:
            return self._filters[selection]
        elif selection == '':
            return reduce(operator.and_, self._filters.values())
        else:
            return Eval(self._filters)[selection]

    def __call__(self, *indices: AnyArray, selection: str = ''):
        return self[selection](*indices)