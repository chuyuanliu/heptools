from __future__ import annotations

import itertools
from functools import cache
from math import comb, perm

import awkward as ak
import numpy as np
from awkward import Array


@cache
def _generate_combination(size: int, groups: int, members: int) -> np.ndarray:
    _total, _groups = groups * members, []
    if (size < _total):
        return np.empty((0, groups, members), dtype = np.int0)
    _combs = itertools.combinations(np.arange(size), members)
    if groups == 1:
        return np.array(list([list(_comb)] for _comb in _combs))
    for _comb in _combs:
        _comb = list(_comb)
        if (size - _comb[0]) >= _total:
            _remain   = np.array([], dtype = np.int0)
            _interval = _comb + [size]
            for i in range(members):
                _remain = np.concatenate((_remain, np.arange(_interval[i] + 1, _interval[i + 1]).astype(np.int0)))
            _last = _remain[_generate_combination(_remain.shape[0], groups - 1, members)]
            _first = np.repeat(np.array([_comb]), partition_count(_remain.shape[0], groups - 1, members), axis = 0)[:, np.newaxis, :]
            _groups.append(np.concatenate((_first, _last), axis = 1))
    return np.concatenate(_groups)

def partition_count(size: int, groups: int, members: int) -> int:
    if size < groups * members:
        return 0
    else:
        _count, _groups, _remain  = 1, 0, size
        while _remain > 0 and _groups < groups:
            _count *= comb(_remain, members)
            _remain -= members
            _groups += 1
        return _count // perm(groups, groups)

def partition(data: Array, groups: int, members: int) -> tuple[Array, ...]:
    _sizes = ak.num(data)
    assert(ak.all(_sizes >= groups * members))
    _combs = ak.Array([_generate_combination(i, groups, members) for i in range(ak.max(_sizes) + 1)])[_sizes]
    _combs = tuple(ak.unflatten(data[ak.flatten(_combs[:, :, :, i], axis = 2)], groups, axis=1) for i in range(members))
    return _combs