from __future__ import annotations

from fractions import Fraction
from functools import cache, cached_property
from itertools import combinations
from math import comb, perm, prod
from typing import Iterable, Literal, overload

import numpy as np
import numpy.typing as npt

from .sequence import Josephus

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

    @cached_property
    def combination(self) -> list[npt.NDArray[np.int_]]:
        if self.count == 0:
            return list(
                np.empty((0, self.groups[i], self.members[i]), dtype=int)
                for i in range(len(self.groups))
            )
        result = [Partition._combination(self.size, self.groups[0], self.members[0])]
        for i in range(1, len(self.groups)):
            result.append(
                Partition.__setdiff2d(np.arange(self.size), *result[:i])[
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

    def __setdiff2d(index: npt.NDArray, *exclude: npt.NDArray):
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
    def jit():
        from numba import njit

        Partition.__setdiff2d = njit(Partition.__setdiff2d)


class SubPartitionByFraction:
    def __init__(
        self,
        count: int,
        fraction: float | str,
        precision: int = 10,
        method: Literal["greedy", "step"] = "greedy",
    ):
        self._fraction = Fraction(fraction).limit_denominator(precision)
        _granularity = 1
        while (
            comb(
                self._fraction.denominator * _granularity,
                self._fraction.numerator * _granularity,
            )
            < count
        ):
            _granularity += 1
        self._count = count
        self._partition = Partition(
            _granularity * self._fraction.denominator,
            1,
            _granularity * self._fraction.numerator,
        )
        self._method = method

    @property
    def count(self):
        return self._count

    @cached_property
    def combination(self):
        combs = self._partition.combination[0]
        if len(combs) == self._count:
            return combs
        elif self._method == "greedy":
            return combs[[*self._distance_greedy(combs, self._count)]]
        elif self._method == "step":
            return combs[
                Josephus(len(combs), self._partition.size).sequence(self._count)
            ]

    @property
    def fraction(self):
        return self._fraction

    @cached_property
    def multiplicity(self):
        return int(np.ceil(self._fraction * self._count))

    @staticmethod
    def _distance_greedy(combs: npt.NDArray, count: int):
        target = {0}
        remain = {*range(1, combs.shape[0])}
        for _ in range(1, count):
            distance = SubPartitionByFraction.__distance(
                combs, np.array([*remain]), np.array([*target])
            )
            selected = int(distance[:, 0][np.argmin(distance[:, 1])])
            target.add(selected)
            remain.remove(selected)
        return target

    def __distance(
        combs: npt.NDArray, remain: npt.NDArray, target: npt.NDArray
    ) -> npt.NDArray:
        d = np.empty((len(remain), 2), dtype=np.float64)
        size = combs.shape[-1] * 2
        for i, rem in enumerate(remain):
            ds = np.empty(len(target), dtype=np.float64)
            for j, tar in enumerate(target):
                ds[j] = np.unique(np.concatenate((combs[rem], combs[tar]))).shape[0]
            d[i, 0] = rem
            d[i, 1] = size - np.mean(ds)
        return d

    @staticmethod
    def jit():
        from numba import njit

        SubPartitionByFraction.__distance = njit(SubPartitionByFraction.__distance)
