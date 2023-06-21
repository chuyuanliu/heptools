from __future__ import annotations

import operator
from functools import reduce
from typing import Iterable

import numpy as np
from coffea.processor.accumulator import accumulate

from .._utils import Eval
from .container import PartialBoolArray


class Selection:
    def __init__(self, **filters: PartialBoolArray):
        self._filters: dict[str, PartialBoolArray] = filters

    def add(self, selection: str, index: Iterable[int], value: bool | Iterable[bool]):
        value = PartialBoolArray(index, value)
        self._filters = accumulate((self._filters, {selection: value}))
        return self

    def __add__(self, other: Selection) -> Selection:
        if isinstance(other, Selection):
            return Selection(**accumulate((self._filters, other._filters)))
        else:
            return NotImplemented

    def __getitem__(self, selection: str) -> PartialBoolArray:
        if selection in self._filters:
            return self._filters[selection]
        elif selection == '':
            return reduce(operator.and_, self._filters.values())
        else:
            return Eval(self._filters)[selection]

    def __call__(self, index: Iterable[np.uint], selection: str = '', bounded = True):
        return self[selection](index, bounded)