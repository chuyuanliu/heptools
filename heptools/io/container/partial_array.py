from __future__ import annotations

from typing import Iterable

import numpy as np
import numpy.typing as npt

IndexType = np.uint64

class PartialBoolArray:
    _uint8_bit_sum = None

    @staticmethod
    def _bit_align(x: IndexType):
        return IndexType(x) & ~IndexType(7)

    @staticmethod
    def _bit_padding(x: bool):
        return ~np.uint8(0) if x else np.uint8(0)

    def _bit_index(self, index: npt.NDArray[np.uint]) -> tuple[npt.NDArray[np.uint], npt.NDArray[np.uint8]]:
        if self._start != 0:
            index -= self._start
        offset = np.asarray(index, dtype = np.uint8) & np.uint8(7)
        index //= IndexType(8)
        return index, offset

    def _bit_value(self, index: npt.NDArray[np.uint]) -> npt.NDArray[np.uint8]:
        index, offset = self._bit_index(index)
        return (self._value[index] >> offset) & np.uint8(1)

    def __init__(self, index: Iterable[np.uint] = None, value: bool | Iterable[bool] = True, default = False):
        index = np.array(index)
        value = np.asarray(value, dtype = np.bool_)
        self._start, self._end = self._bit_align(index.min()), self._bit_align(index.max()) + IndexType(7)
        self._default = default
        index, offset = self._bit_index(index)
        self._value = np.full(self.shape//IndexType(8), self._bit_padding(self._default), dtype = np.uint8)
        if not self._default:
            np.bitwise_or.at(self._value, index[value], (np.uint8(1) << offset[value]))
        else:
            np.bitwise_and.at(self._value, index[~value], ~(np.uint8(1) << offset[~value]))

    def __invert__(self):
        obj = object.__new__(self.__class__)
        obj._start = self._start
        obj._end   = self._end
        obj._value = ~self._value
        obj._default = not self._default
        return obj

    def _bitwise(self, other: PartialBoolArray, op, iop):
        if isinstance(other, PartialBoolArray):
            def _iop(buffer, value1, value2):
                buffer[:] = value1
                iop(buffer, value2)
            edge = np.array([[self._start, self._end], [other._start, other._end]], dtype = IndexType)
            obj = object.__new__(self.__class__)
            obj._start, obj._end = edge[:, 0].min(), edge[:, 1].max()
            obj._default = op(self._default, other._default)
            overlap = (edge[:, 0].max(), edge[:, 1].min())
            value   = (self._value, other._value)
            shape   = (self.shape >> IndexType(3), other.shape >> IndexType(3), obj.shape >> IndexType(3))
            padding = (self._bit_padding(self._default), self._bit_padding(other._default))
            if (edge[0] == edge[1]).all():
                obj._value = op(value[0], value[1])
            elif overlap[0] > overlap[1]:
                order = edge[:, 0].argsort()
                obj._value = np.full(shape[2], op(padding[0], padding[1]), dtype = np.uint8)
                _iop(obj._value[:len(value[order[0]])], value[order[0]], padding[order[1]])
                _iop(obj._value[-len(value[order[1]]):], value[order[1]], padding[order[0]])
            else:
                start_o = edge[:, 0].argsort()
                end_o   = edge[:, 1].argsort()
                obj._value = np.empty(shape[2], dtype = np.uint8)
                before  = edge[:, 0].ptp() >> IndexType(3)
                after   = edge[:, 1].ptp() >> IndexType(3)
                between = shape[2] - before - after
                if before > 0:
                    _iop(obj._value[:before], padding[start_o[1]], value[start_o[0]][:before])
                if between > 0:
                    _iop(obj._value[before:shape[2]-after], value[start_o[1]][0:between], value[start_o[0]][before:before + between])
                if after > 0:
                    _iop(obj._value[shape[2]-after:], value[end_o[1]][shape[end_o[1]]-after:], padding[end_o[0]])
            return obj
        else:
            return NotImplemented

    def __and__(self, other: PartialBoolArray):
        return self._bitwise(other, np.bitwise_and , np.ndarray.__iand__)

    def __or__(self, other: PartialBoolArray):
        return self._bitwise(other, np.bitwise_or, np.ndarray.__ior__)

    def __xor__(self, other: PartialBoolArray):
        return self._bitwise(other, np.bitwise_xor, np.ndarray.__ixor__)

    def __add__(self, other: PartialBoolArray):
        return self | other

    @property
    def shape(self):
        return self._end - self._start + IndexType(1)

    def __len__(self):
        return self.shape

    def get(self, index: Iterable[np.uint], bounded = True):
        index = np.array(index)
        if bounded:
            return self._bit_value(index).astype(np.bool_)
        else:
            out = np.full(len(index), self._default, dtype = np.bool_)
            stored = (index >= self._start) & (index <= self._end)
            out[stored] = self._bit_value(index[stored]).astype(np.bool_)
            return out

    def __call__(self, index: Iterable[np.uint], bounded: bool = True):
        return self.get(index, bounded)

    @property
    def count(self):
        if PartialBoolArray._uint8_bit_sum is None:
            PartialBoolArray._uint8_bit_sum = np.unpackbits(np.arange(256, dtype = np.uint8)).reshape(-1, 8).sum(axis = 1)
        return PartialBoolArray._uint8_bit_sum[self._value].sum()