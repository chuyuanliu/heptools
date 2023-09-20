from __future__ import annotations

from typing import Iterable

import awkward as ak
import numpy as np

from ..aktools import AnyArray


# TODO numba
class PartialSet:
    sort_kind = 'mergesort'

    def _zip(self, *indices: AnyArray):
        return ak.zip(indices) if len(indices) > 1 else indices[0]

    def __init__(self, value: bool | Iterable[bool], *indices: AnyArray, default = False):
        indices = np.array(self._zip(*indices))
        self._in = np.empty(0, dtype = indices.dtype)
        self._out = default
        if isinstance(value, bool):
            if value != default:
                self._in = indices
        else:
            self._in = indices[value != default]
        self._in.sort(kind = self.sort_kind)

    @classmethod
    def new(cls, _in: np.ndarray, _out: bool, sort: bool = False):
        new = object.__new__(cls)
        new._in = _in
        new._out = _out
        if sort:
            new._in.sort(kind = cls.sort_kind)
        return new

    def __invert__(self):
        return self.new(self._in, not self._out)

    def __and__(self, other: PartialSet) -> PartialSet: # TODO check type of self._in and other._in
        if isinstance(other, PartialSet):
            if self._out:
                if other._out:
                    _in = np.union1d(self._in, other._in)
                else:
                    _in = np.setdiff1d(other._in, self._in)
            else:
                if other._out:
                    _in = np.setdiff1d(self._in, other._in)
                else:
                    _in = self._in[np.isin(self._in, other._in)]
            return self.new(_in, self._out and other._out, True)
        return NotImplemented

    def __or__(self, other: PartialSet) -> PartialSet:
        if isinstance(other, PartialSet):
            return ~(~self & ~other)
        return NotImplemented

    def __xor__(self, other: PartialSet) -> PartialSet:
        if isinstance(other, PartialSet):
            return self.new(np.setxor1d(self._in, other._in, assume_unique = True), self._out is not other._out, True)
        return NotImplemented

    def __add__(self, other: PartialSet) -> PartialSet:
        if isinstance(other, PartialSet):
            if not self._out == other._out:
                raise ValueError('cannot add two partial sets with different default values')
            return self.new(np.concatenate((self._in, other._in)), self._out or other._out, True)
        return NotImplemented

    def __call__(self, *indices: AnyArray):
        indices = np.array(self._zip(*indices))
        return np.isin(indices, self._in, invert = self._out)

    def __len__(self):
        return len(self._in)

    def unique(self):
        self._in, count = np.unique(self._in, return_counts = True)
        return self._in[count > 1]