from __future__ import annotations

from typing import Generator, Iterable

import numpy as np
import numpy.typing as npt

from ..typetools import check_type
from ..utils import unpack


class CouplingError(Exception):
    __module__ = Exception.__module__


class Coupling:
    def __init__(self, kappas: KappaList):
        if isinstance(kappas, Diagram) or (
            isinstance(kappas, type) and issubclass(kappas, Diagram)
        ):
            kappas = kappas.diagrams[0]
        elif isinstance(kappas, Coupling):
            kappas = kappas._ks
        else:
            kappas = tuple(kappas)
        self._ks = kappas
        self._cs = np.empty([0, len(kappas)])

    def append(self, couplings: np.ndarray):
        couplings = np.asarray(couplings)
        if couplings.shape[-1] != len(self._ks):
            raise CouplingError(
                f"Coupling shape {couplings.shape} does not match (..., {len(self._ks)})"
            )
        self._cs = np.concatenate((self._cs, couplings), axis=0)
        return self

    def meshgrid(self, default: float = 1, **kwargs):
        self.append(
            np.stack(
                np.meshgrid(*[kwargs.get(kappa, default) for kappa in self._ks]),
                axis=-1,
            ).reshape([-1, len(self._ks)])
        )
        return self

    def reshape(self, kappas: KappaList, default: float = 1):
        new = Coupling(kappas)
        if new._ks == self._ks:
            new._cs = self._cs.copy()
        else:
            couplings = []
            for kappa in new._ks:
                try:
                    idx = self._ks.index(kappa)
                    couplings.append(self._cs[:, idx : idx + 1])
                except ValueError:
                    couplings.append(np.full([self._cs.shape[0], 1], default))
            new._cs = np.concatenate(couplings, axis=-1)
        return new

    def __iadd__(self, other) -> Coupling:
        if isinstance(other, Coupling):
            if self._ks == other._ks:
                to_add = other._cs
            else:
                to_add = other.reshape(self)._cs
            self.append(to_add)
            return self
        return NotImplemented

    def __len__(self):
        return len(self._cs)

    def __getitem__(self, idx) -> dict[str, npt.NDArray]:
        if isinstance(idx, int):
            return dict(zip(self._ks, self._cs[idx]))
        return Coupling(self._ks).append(self._cs[idx])

    def __iter__(self) -> Generator[dict[str, float]]:
        for i in range(len(self)):
            yield self[i]

    def __repr__(self):
        return "\n".join(
            [",".join(self._ks)]
            + [",".join(str(value) for value in self._cs[i]) for i in range(len(self))]
        )


class _DiagramMeta(type):
    diagrams: tuple[tuple[str, ...], tuple[tuple[int, ...], ...]]

    def __setattr__(self, name: str, value):
        if name == "diagrams":
            raise CouplingError("Cannot overwrite diagrams")
        super().__setattr__(name, value)


class Diagram(metaclass=_DiagramMeta):
    def __init_subclass__(cls):
        if not hasattr(cls, "diagrams"):
            raise CouplingError("Diagram is not defined")
        else:
            if not check_type(
                cls.diagrams, tuple[tuple[str, ...], tuple[tuple[int, ...], ...]]
            ):
                raise CouplingError("Invalid diagram type")
        super().__init_subclass__()

    def __init__(self, basis: npt.ArrayLike, unit_basis_weight=True):
        self.unit_basis_weight = unit_basis_weight
        basis = np.asarray(basis)
        n = len(self.diagrams[0])
        self._basis = Coupling(self.diagrams[0]).append(basis[:, :n])
        self._xs = basis[:, n : n + 1].flatten()
        self._unc = basis[:, n + 1 : n + 2].flatten()
        self._transmat = np.linalg.pinv(self._component_scale(self._basis._cs))
        _s = self._transmat.shape
        if _s[1] < _s[0]:
            raise CouplingError(f"Require more couplings ({_s[1]}/{_s[0]} provided)")

    def _component_scale(self, couplings: npt.ArrayLike):
        couplings = np.asarray(couplings)[:, :, np.newaxis]
        diagram = np.asarray(self.diagrams[1]).T[np.newaxis, :, :]
        idx2 = np.stack(np.tril_indices(diagram.shape[-1]), axis=-1)
        diagram2 = np.unique(np.sum(diagram[:, :, idx2], axis=-1), axis=-1)
        return np.prod(np.power(couplings, diagram2), axis=1)

    def weight(self, couplings: Coupling):
        couplings = couplings.reshape(self)._cs
        weight = self._component_scale(couplings) @ self._transmat
        if self.unit_basis_weight:
            matched_basis = (couplings == self._basis._cs[:, np.newaxis]).all(-1).T
            is_basis = matched_basis.any(-1)
            weight[is_basis] = matched_basis[is_basis]
        return weight

    def xs(self, couplings: Coupling):
        if self._xs.shape[0] == 0:
            raise CouplingError("Cross section is not provided")
        return unpack(self.weight(couplings) @ self._xs)

    def xs_unc(self, couplings: Coupling):
        if self._unc.shape[0] == 0:
            raise CouplingError("Uncertainty is not provided")
        return unpack(np.sqrt(self.weight(couplings) ** 2 @ self._unc**2))


KappaList = Coupling | Diagram | type[Diagram] | Iterable[str]
