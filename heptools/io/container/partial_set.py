from __future__ import annotations

from typing import Iterable

import awkward as ak
import numpy as np

from ...aktools import Sliceable


class PartialSet:
    check_unique = False
    sort_kind = 'mergesort'

    def __init__(self, value: bool | Iterable[bool], *indices: Sliceable, default = False):
        indices = np.array(ak.zip(indices))
        if self.check_unique:
            if not np.all(np.unique(indices, return_counts = True)[1] == 1):
                raise IOError('indices are not unique')
        self._in = np.empty(0, dtype = indices.dtype)
        self._out = default
        if isinstance(value, bool):
            if value != default:
                self._in = indices
        else:
            self._in = indices[value != default]
        self._in.sort(kind = self.sort_kind)

    def __invert__(self):
        new = object.__new__(self.__class__)
        new._in = self._in
        new._out = ~self._out
        return new

    def __and__(self, other: PartialSet):
        if self._out:
            if other._out:
                _in = np.unique(np.concatenate((self._in, other._in)))
            else:
                _in = np.setdiff1d(other._in, self._in)
        else:
            if other._out:
                _in = np.setdiff1d(self._in, other._in)
            else:
                _in = self._in[np.isin(self._in, other._in)]
        _in.sort(kind = self.sort_kind)
        new = object.__new__(self.__class__)
        new._in = _in
        new._out = self._out & other._out
        return new

    def __or__(self, other: PartialSet):
        return ~(~self & ~other)

    def __xor__(self, other: PartialSet):
        _v, _c = np.unique(np.concatenate((self._in, other._in)), return_counts = True)
        _in = _v[_c == 1]
        _in.sort(kind = self.sort_kind)
        new = object.__new__(self.__class__)
        new._in = _in
        new._out = self._out ^ other._out
        return new

    def __add__(self, other: PartialSet):
        return self | other

    def __call__(self, *indices: Sliceable):
        indices = np.array(ak.zip(indices))
        return np.isin(indices, self._in, invert = self._out)

    def __len__(self):
        return len(self._in)