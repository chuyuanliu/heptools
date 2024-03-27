from __future__ import annotations

from fractions import Fraction
from functools import cache, cached_property
from itertools import combinations
from math import comb, perm, prod
from typing import Iterable, Literal, overload

import numpy as np
import numpy.typing as npt

from .jit import Compilable, allow_jit

__all__ = ["Partition"]


class Partition(Compilable):
    @overload
    def __init__(self, size: int, groups: int, members: int): ...
    @overload
    def __init__(self, size: int, groups: Iterable[int]): ...
    def __init__(self, size: int, groups: int | Iterable[int], members: int = None):
        self._size = size
        if isinstance(groups, Iterable):
            self._members, self._groups = np.unique(
                np.asarray(groups, int), return_counts=True
            )
        else:
            self._members = np.array([members], dtype=int)
            self._groups = np.array([groups], dtype=int)
        if size < np.sum(self._groups * self._members):
            self._n_combs = 0
        else:
            if len(self._groups) == 1:
                self._n_combs = Partition._count(
                    self._size, self._groups[0], self._members[0]
                )
            else:
                self._subs, self._n_combs, size = list[Partition](), 1, self._size
                for i in range(0, len(self._groups)):
                    self._subs.append(
                        Partition(size, self._groups[i], self._members[i])
                    )
                    self._n_combs *= self._subs[i]._n_combs
                    size -= self._groups[i] * self._members[i]

    @property
    def n_combinations(self) -> int:
        return self._n_combs

    @cached_property
    def combinations(self) -> list[npt.NDArray[np.int_]]:
        if self.n_combinations == 0:
            return list(
                np.empty((0, self._groups[i], self._members[i]), dtype=int)
                for i in range(len(self._groups))
            )
        result = [Partition._combination(self._size, self._groups[0], self._members[0])]
        for i in range(1, len(self._groups)):
            result.append(
                Partition.__setdiff2d(np.arange(self._size), *result[:i])[
                    :, self._subs[i].combinations[0]
                ].reshape((-1, self._groups[i], self._members[i]))
            )
            for j in range(i):
                result[j] = np.repeat(result[j], self._subs[i]._n_combs, axis=0)
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
            end = start + partition._n_combs
            partitions[start:end, 0, :] = c
            partitions[start:end, 1:, :] = remain[partition.combinations]
            start = end
        return partitions

    @allow_jit
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


class SubPartitionByFraction(Compilable):
    def __init__(
        self,
        max_combinations: int,
        fraction: float | str | Fraction,
        method: Literal["greedy"] = "greedy",
    ):
        self._n_combs = max_combinations
        self._fraction = Fraction(fraction)
        self._method = method
        self._fineness = 1
        while (
            comb(
                self.fraction.denominator * self._fineness,
                self.fraction.numerator * self._fineness,
            )
            < self.n_combinations
        ):
            self._fineness += 1
        self._partition = Partition(
            self._fineness * self.fraction.denominator,
            1,
            self._fineness * self.fraction.numerator,
        )

    @property
    def n_combinations(self):
        return self._n_combs

    @cached_property
    def combinations(self):
        combs = self._partition.combinations[0]
        if len(combs) == self.n_combinations:
            return combs
        match self._method:
            case "greedy":
                return combs[self._greedy(combs, self.n_combinations)]

    @cached_property
    def granularity(self):
        return self._fineness * self.fraction.denominator

    @property
    def fraction(self):
        return self._fraction

    @cached_property
    def multiplicity(self):
        return dict(zip(*np.unique(self.combinations, return_counts=True)))

    @cached_property
    def max_multiplicity(self):
        return int(np.ceil(self.fraction * self.n_combinations))

    @staticmethod
    def _greedy(combs: npt.NDArray, count: int):
        result = np.zeros(count, dtype=int)
        remain = [*range(1, combs.shape[0])]
        for i in range(1, count):
            distance = SubPartitionByFraction.__metric(
                combs, np.array(remain), result[:i]
            )
            selected = remain[np.argmin(distance)]
            result[i] = selected
            remain.remove(selected)
        return sorted(result)

    @allow_jit
    def __metric(combs: npt.NDArray, source: npt.NDArray, target: npt.NDArray):
        distance = np.empty(len(source), dtype=np.float64)
        size = combs.shape[-1] * 2
        for m, i in enumerate(source):
            ds = np.empty(len(target), dtype=np.float64)
            for n, f in enumerate(target):
                ds[n] = np.unique(np.concatenate((combs[i], combs[f]))).shape[0]
            distance[m] = size - np.mean(ds)
        return distance
