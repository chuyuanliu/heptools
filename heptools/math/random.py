from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from typing import Iterable, Literal, overload

import numpy as np
import numpy.typing as npt

_UINT64_MAX_52BITS = np.float64(1 << 53)
_UINT64_32 = np.uint64(32)
_UINT64_11 = np.uint64(11)


SeedLike = int | str | Iterable[int | str]


def _str_to_entropy(__str: str) -> list[np.uint32]:
    return np.frombuffer(hashlib.md5(__str.encode()).digest(), dtype=np.uint32).tolist()


def _seed(entropy: SeedLike):
    if isinstance(entropy, str):
        return _str_to_entropy(entropy)
    elif isinstance(entropy, Iterable):
        seeds = []
        for i in entropy:
            if isinstance(i, str):
                seeds.extend(_str_to_entropy(i))
            else:
                seeds.append(i)
        return seeds
    else:
        return entropy


class CBRNG(ABC):
    @abstractmethod
    def bit32(self, counters: npt.NDArray[np.uint64]) -> npt.NDArray[np.uint32]: ...

    @abstractmethod
    def bit64(self, counters: npt.NDArray[np.uint64]) -> npt.NDArray[np.uint64]: ...

    @overload
    def uint(
        self, counters: npt.ArrayLike, bits: Literal[64] = 64
    ) -> npt.NDArray[np.uint64]: ...
    @overload
    def uint(
        self, counters: npt.ArrayLike, bits: Literal[32] = 32
    ) -> npt.NDArray[np.uint32]: ...
    def uint(
        self, counters: npt.ArrayLike, bits: Literal[32, 64] = 64
    ) -> npt.NDArray[np.uint]:
        counters = np.asarray(counters, dtype=np.uint64)
        match bits:
            case 32:
                return self.bit32(counters)
            case 64:
                return self.bit64(counters)
            case _:
                raise NotImplementedError

    @overload
    def float(
        self, counters: npt.ArrayLike, bits: Literal[64] = 64
    ) -> npt.NDArray[np.float64]: ...
    def float(
        self, counters: npt.ArrayLike, bits: Literal[64] = 64
    ) -> npt.NDArray[np.float_]:
        """
        In [0, 1). Same as `numpy.random._common.uint64_to_double`.
        """
        match bits:
            case 64:
                x = self.uint(counters)
                x >>= _UINT64_11
                return x / _UINT64_MAX_52BITS
            case _:
                raise NotImplementedError


class Squares(CBRNG):
    """
    Squares: a counter-based random number generator (CBRNG) [1]_.

    .. [1] https://arxiv.org/abs/2004.06278
    """

    @classmethod
    def _generate_key(cls, seed: SeedLike) -> np.uint64:
        gen = np.random.Generator(np.random.MT19937(_seed(seed)))
        bits = np.arange(1, 16, dtype=np.uint64)
        offsets = np.arange(0, 29, 4, dtype=np.uint64)
        lower8 = gen.choice(bits, 8, replace=False)
        for i in range(16):
            if lower8[i] % 2 == 1:
                lower8 = np.roll(lower8, -i)
                break
        higher8 = np.zeros(8, dtype=np.uint64)
        higher8[0] = gen.choice(np.delete(bits, int(lower8[-1]) - 1), 1)
        higher8[1:] = gen.choice(np.delete(bits, int(higher8[0]) - 1), 7, replace=False)
        return np.sum(lower8 << offsets) + (np.sum(higher8 << offsets) << _UINT64_32)

    @classmethod
    def _round(cls, LR: npt.NDArray, shift: npt.NDArray, last: bool = False):
        LR *= LR
        LR += shift
        if last:
            yield LR.copy()
        L = LR >> _UINT64_32
        LR <<= _UINT64_32
        LR |= L
        yield LR

    @property
    def key(self) -> np.uint64:
        return self._key

    def __init__(self, seed: SeedLike):
        self._key = self._generate_key(seed)

    def bit32(self, ctrs: npt.NDArray[np.uint64]) -> npt.NDArray[np.uint32]:
        x = ctrs * self._key
        y = x.copy()
        z = y + self._key
        # round 1-3
        for i in [y, z, y]:
            (_,) = self._round(x, i)
        # round 4
        x *= x
        x += z
        x >>= _UINT64_32
        return x.astype(np.uint32)

    def bit64(self, ctrs: npt.NDArray[np.uint64]) -> npt.NDArray[np.uint64]:
        x = ctrs * self._key
        y = x.copy()
        z = y + self._key
        # round 1-3
        for i in [y, z, y]:
            (_,) = self._round(x, i)
        # round 4
        (t, _) = self._round(x, z, last=True)
        # round 5
        x *= x
        x += y
        x >>= _UINT64_32
        x ^= t
        return x


class Philox(CBRNG):
    """
    Philox: a counter-based random number generator (CBRNG) [1]_.

    .. [1] https://doi.org/10.1145/2063384.2063405
    """

    # TODO: implement Philox
