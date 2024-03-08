import numpy as np
import numpy.typing as npt

__all__ = ["Metric", "Binary"]


class Prefix:
    dtype = np.float64

    def __init__(self, base: int, prefix: list[str], range: tuple[int, int]):
        self.base = self.dtype(base)
        self.prefix = np.asarray(prefix)
        self.range = np.asarray(range)

    def add(self, value: npt.NDArray):
        value = np.asarray(value, dtype=self.dtype)
        with np.errstate(divide="ignore"):
            power = np.floor(
                np.nan_to_num(np.log(np.abs(value)), False, 0, 0, 0) / np.log(self.base)
            ).astype(int)
        power = np.clip(power, *self.range)
        return (
            np.asarray(value / np.power(self.base, power), dtype=self.dtype),
            self.prefix[power],
        )

    def remove(self, value: npt.NDArray[np.unicode_]):
        value = np.asarray(value, dtype=np.unicode_)
        power = np.zeros(value.shape, dtype=int)
        for i in range(1, len(self.prefix)):
            prefix_power = i if i <= self.range[1] else i - len(self.prefix)
            matched = np.char.endswith(value, self.prefix[i])
            power[matched] = prefix_power
            value[matched] = np.char.rstrip(value[matched], self.prefix[i])
        return np.asarray(
            value.astype(self.dtype) * np.power(self.base, power), dtype=self.dtype
        )


Metric = Prefix(1000, ["", *"kMGTPEZYyzafpnμm"], (-8, 8))
Binary = Prefix(1024, [""] + [f"{i}i" for i in "KMGTPEZY"], (0, 8))
