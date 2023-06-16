import numpy as np


class Prefix:
    def __init__(self, base: int, prefix: list[str], range: tuple[int, int]):
        self.base   = np.log(base)
        self.prefix = np.asarray(prefix)
        self.range  = np.asarray(range)

    def __call__(self, value):
        value = np.asarray(value, dtype=np.float64)
        with np.errstate(divide='ignore'):
            power = (np.nan_to_num(np.log(np.abs(value)), False, 0, 0, 0) / self.base).astype(int)
        power = np.clip(power, *self.range)
        return np.asarray(value / np.exp(self.base * power), dtype=np.float64), self.prefix[power]

Metric = Prefix(1000, ['', *'kMGTPEZYyzafpnmμm'], (-8, 8))
Binary = Prefix(1024, ['', *'KMGTPEZY'], (0, 8))