from __future__ import annotations

from typing import Callable, Iterable, overload

import numpy as np
import numpy.typing as npt
import pandas as pd

from ..typetools import check_type
from ..utils import unpack


def _mesh(*xi: npt.NDArray) -> Iterable[npt.NDArray]:
    return map(np.ravel, np.meshgrid(*xi, indexing="ij"))


class CouplingError(Exception):
    __module__ = Exception.__module__


class Coupling:
    default: float = 1.0

    def __init__(self, *tables: TableLike, **columns: ColumnLike):
        self.__ds: list[pd.DataFrame] = []
        self.extend(*tables, **columns)

    @property
    def _d(self) -> pd.DataFrame:
        if len(self.__ds) > 1:
            self.__ds = [
                pd.concat(
                    self.__ds,
                    axis=0,
                    ignore_index=True,
                    sort=False,
                    copy=False,
                ).fillna(self.default)
            ]
        return self.__ds[0]

    def _df(self, table: TableLike) -> pd.DataFrame:
        match table:
            case Coupling():
                return table._d
            case pd.DataFrame():
                return table
            case dict():
                return pd.DataFrame(
                    {
                        k: v if isinstance(v, Iterable) else (v,)
                        for k, v in table.items()
                    }
                )

    def _op(self, op: Callable, k: dict):
        return self.extend(dict(zip(k, op(*k.values()))))

    def extend(self, *tables: TableLike, **columns: ColumnLike):
        if tables:
            self.__ds.extend(map(self._df, tables))
        if columns:
            self.__ds.append(self._df(columns))
        return self

    def broadcast(self, **columns: ColumnLike):
        return self._op(np.broadcast_arrays, columns)

    def cartesian(self, **columns: ColumnLike):
        return self._op(_mesh, columns)

    def copy(self, *columns: str):
        columns = columns or self._d.columns
        new = Coupling()
        new.__ds.append(
            pd.DataFrame(
                np.full((len(self), len(columns)), self.default),
                columns=columns,
            )
        )
        for c in columns:
            if c in self:
                new.loc[:, c] = self.loc[:, c]
        return new

    def array(self, *columns: str, allow_missing: bool = True) -> npt.NDArray:
        if columns not in self:
            if not allow_missing:
                missing = set(columns) - set(self._d.columns)
                raise CouplingError(f"{missing} are not provided.")
            data = self.copy(*columns)._d
        else:
            data = self.loc[:, columns]
        return data.to_numpy()

    def __add__(self, other) -> Coupling:
        if check_type(other, TableLike):
            new = Coupling()
            new.__ds.extend(self.__ds)
            if isinstance(other, Coupling):
                new.__ds.extend(other.__ds)
            else:
                new.extend(other)
            return new
        return NotImplemented

    def __len__(self):
        return len(self._d)

    def __repr__(self):
        return repr(self._d)

    def __iter__(self):
        for idx in range(len(self)):
            yield self[idx]

    def __contains__(self, keys: str | Iterable[str]):
        if isinstance(keys, str):
            return keys in self._d
        return all(k in self._d for k in keys)

    @overload
    def __getitem__(self, idx: int) -> dict[str, float]: ...
    @overload
    def __getitem__(self, idx: slice) -> Coupling: ...
    def __getitem__(self, idx):
        if isinstance(idx, int):
            return self._d.loc[idx % len(self)].to_dict()
        elif isinstance(idx, slice):
            new = Coupling()
            d = self._d[idx]
            d.reset_index(drop=True, inplace=True)
            new.__ds.append(d)
            return new

    @property
    def loc(self):
        return self._d.loc


TableLike = dict[str, float | Iterable[float]] | pd.DataFrame | Coupling
ColumnLike = float | Iterable[float]


class _DiagramMeta(type):
    diagrams: tuple[tuple[str, ...], tuple[tuple[int, ...], ...]]

    def __setattr__(self, name: str, value):
        if name == "diagrams":
            raise CouplingError("Cannot overwrite diagrams")
        super().__setattr__(name, value)


class Diagram(metaclass=_DiagramMeta):
    __diagram2: npt.NDArray

    def __init_subclass__(cls):
        if not hasattr(cls, "diagrams"):
            raise CouplingError("Diagram is not defined")
        else:
            if not check_type(
                cls.diagrams, tuple[tuple[str, ...], tuple[tuple[int, ...], ...]]
            ):
                raise CouplingError("Invalid diagram type")
        super().__init_subclass__()
        diagram = np.asarray(cls.diagrams[1]).T[np.newaxis, ...]
        indices = np.stack(np.tril_indices(diagram.shape[-1]), axis=-1)
        cls.__diagram2 = np.unique(np.sum(diagram[..., indices], axis=-1), axis=-1)

    def __init__(self, basis: Coupling, unit_basis_weight=False):
        size_b, min_b = len(basis), self.__diagram2.shape[-1]
        if size_b < min_b:
            raise CouplingError(f"Need more basis ({size_b}/{min_b} provided)")
        self._data = basis
        self._unit = unit_basis_weight
        self._basis = basis.array(*self.diagrams[0])
        self._transmat = np.linalg.pinv(self._scale(self._basis))

    def _scale(self, couplings: npt.NDArray) -> npt.NDArray:
        return np.prod(np.power(couplings[:, :, np.newaxis], self.__diagram2), axis=1)

    def _arr1d(self, var: str):
        return self._data.array(var, allow_missing=False).ravel()

    def weight(self, couplings: Coupling):
        values = couplings.array(*self.diagrams[0])
        weight = self._scale(values) @ self._transmat
        if self._unit:
            matched_basis = (values == self._basis[:, np.newaxis]).all(-1).T
            is_basis = matched_basis.any(-1)
            weight[is_basis] = matched_basis[is_basis]
        return weight

    def linear(self, variable: str, couplings: Coupling):
        return unpack(self.weight(couplings) @ self._arr1d(variable))

    def quadratic(self, variable: str, couplings: Coupling):
        return unpack(np.sqrt(self.weight(couplings) ** 2 @ self._arr1d(variable) ** 2))

    def xs(self, couplings: Coupling):
        return self.linear("xs", couplings)

    def xs_unc(self, couplings: Coupling):
        return self.quadratic("xs_unc", couplings)
