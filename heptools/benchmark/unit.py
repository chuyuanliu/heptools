import numpy as np
import numpy.typing as npt

__all__ = ['Metric', 'Binary']

class Prefix:
    def __init__(self, base: int, prefix: list[str], range: tuple[int, int]):
        self.base   = np.log(base)
        self.prefix = np.asarray(prefix)
        self.range  = np.asarray(range)

    def add(self, value: npt.NDArray[np.float64]):
        value = np.asarray(value, dtype=np.float64)
        with np.errstate(divide='ignore'):
            power = np.floor(np.nan_to_num(np.log(np.abs(value)), False, 0, 0, 0) / self.base).astype(int)
        power = np.clip(power, *self.range)
        return np.asarray(value / np.exp(self.base * power), dtype=np.float64), self.prefix[power]

    def remove(self, value: npt.NDArray[np.unicode_]):
        value = np.asarray(value, dtype=np.unicode_)
        power = np.zeros(value.shape, dtype=int)
        for i in range(1, len(self.prefix)):
            prefix_power = i if i <= self.range[1] else i - len(self.prefix)
            matched = np.char.endswith(value, self.prefix[i])
            power[matched] = prefix_power
            value[matched] = np.char.rstrip(value[matched], self.prefix[i])
        return np.asarray(value.astype(np.float64) * np.exp(self.base * power), dtype=np.float64)

Metric = Prefix(1000, ['', *'kMGTPEZYyzafpnμm'], (-8, 8))
Binary = Prefix(1024, [''] + [f'{i}i' for i in 'KMGTPEZY'], (0, 8))