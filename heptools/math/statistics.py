from __future__ import annotations

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Generic, TypeVar

_DataT = TypeVar("_DataT")
_VarianceSelf = TypeVar("_VarianceSelf", bound="Variance")
_VarianceData = namedtuple("_VarianceBase", ["sumw", "m1", "M2"])


class Variance(ABC, Generic[_DataT]):
    """
    A model to accumulate the mean and the variance of datasets in a numerically stable way [1]_.

    .. [1] https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
    """

    @classmethod
    @abstractmethod
    def calculate(
        cls, data: _DataT, weight: _DataT = None
    ) -> tuple[_DataT, _DataT, _DataT]: ...

    def __init__(self, data: _DataT = None, weight: _DataT = None):
        if data is None:
            self._raw = _VarianceData(None, None, None)
        else:
            self._raw = _VarianceData(*self.calculate(data, weight))

    @property
    def mean(self) -> _DataT:
        return self._raw.m1

    @property
    def variance(self) -> _DataT:
        return self._raw.M2 / self._raw.sumw

    def __add__(self: _VarianceSelf, other: _VarianceSelf) -> _VarianceSelf:
        if not isinstance(other, type(self)):
            return NotImplemented
        new = type(self)()
        if self._raw.sumw is None:
            new._raw = other._raw
        elif other._raw.sumw is None:
            new._raw = self._raw
        else:
            sumw = self._raw.sumw + other._raw.sumw
            delta = other._raw.m1 - self._raw.m1
            m1 = self._raw.m1 + delta * other._raw.sumw / sumw
            M2 = (
                self._raw.M2
                + other._raw.M2
                + delta**2 * self._raw.sumw * other._raw.sumw / sumw
            )
            new._raw = _VarianceData(sumw, m1, M2)
        return new
