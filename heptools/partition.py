from __future__ import annotations

from functools import cache
from itertools import combinations
from math import comb, perm, prod
from typing import Iterable, overload

import numpy as np
import numpy.typing as npt

__all__ = ["Partition"]


class Partition:
    @overload
    def __init__(self, size: int, groups: int, members: int): ...
    @overload
    def __init__(self, size: int, groups: Iterable[int]): ...
    def __init__(self, size: int, groups: int | Iterable[int], members: int = None):
        self.size = size
        if isinstance(groups, Iterable):
            self.members, self.groups = np.unique(
                np.asarray(groups, int), return_counts=True
            )
        else:
            self.members = np.array([members], dtype=int)
            self.groups = np.array([groups], dtype=int)
        if size < np.sum(self.groups * self.members):
            self.count = 0
        else:
            if len(self.groups) == 1:
                self.count = Partition._count(
                    self.size, self.groups[0], self.members[0]
                )
            else:
                self._subs, self.count, size = list[Partition](), 1, self.size
                for i in range(0, len(self.groups)):
                    self._subs.append(Partition(size, self.groups[i], self.members[i]))
                    self.count *= self._subs[i].count
                    size -= self.groups[i] * self.members[i]

    @property
    def combination(self) -> list[npt.NDArray[np.int_]]:
        if self.count == 0:
            return list(
                np.empty((0, self.groups[i], self.members[i]), dtype=int)
                for i in range(len(self.groups))
            )
        result = [Partition._combination(self.size, self.groups[0], self.members[0])]
        for i in range(1, len(self.groups)):
            result.append(
                Partition._setdiff2d(np.arange(self.size), *result[:i])[
                    :, self._subs[i].combination[0]
                ].reshape((-1, self.groups[i], self.members[i]))
            )
            for j in range(i):
                result[j] = np.repeat(result[j], self._subs[i].count, axis=0)
        return result

    @staticmethod
    @cache
    def _count(size: int, groups: int, members: int) -> int:
        return prod(comb(size - i * members, members) for i in range(groups)) // perm(
            groups, groups
        )

    @staticmethod
    @cache
    def _combination(size: int, groups: int, members: int) -> npt.NDArray[np.int_]:
        if members == 1:
            if groups == 1:
                return np.arange(size)[:, np.newaxis, np.newaxis]
            return Partition._combination(size, members, groups).reshape(
                (-1, groups, members)
            )
        else:
            combs = np.fromiter(
                combinations(np.arange(size), members), dtype=np.dtype((int, members))
            )
        if groups == 1:
            return combs[:, np.newaxis, :]
        combs = combs[combs[:, 0] <= (size - groups * members)]
        start, partitions = 0, np.empty(
            (Partition._count(size, groups, members), groups, members), dtype=int
        )
        for c in combs:
            remain = np.setdiff1d(np.arange(c[0], size), c)
            partition = Partition(remain.shape[0], groups - 1, members)
            end = start + partition.count
            partitions[start:end, 0, :] = c
            partitions[start:end, 1:, :] = remain[partition.combination]
            start = end
        return partitions

    @staticmethod
    def _setdiff2d(index: npt.NDArray, *exclude: npt.NDArray):
        n_exclude, n_index = 0, len(exclude[0])
        for i in np.arange(len(exclude)):
            n_exclude += exclude[i].shape[1]
        result = np.empty((n_index, len(index) - n_exclude), dtype=np.int32)
        for i in np.arange(n_index):
            count = 0
            for j in np.arange(len(index)):
                matched = False
                for k in np.arange(len(exclude)):
                    if index[j] in exclude[k][i]:
                        matched = True
                if not matched:
                    result[i, count] = index[j]
                    count += 1
        return result

    @staticmethod
    def accelerate():
        from numba import njit

        Partition._setdiff2d = njit(Partition._setdiff2d)
