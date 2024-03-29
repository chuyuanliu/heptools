# TODO rewrite, db, json
"""
    kappa-framework
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod, abstractproperty
from functools import cached_property
from string import Formatter
from typing import Generator, overload

import numpy as np
import numpy.typing as npt

from ..utils import unpack


class CouplingError(Exception):
    __module__ = Exception.__module__


class Coupling:
    def __init__(self, kappas: list[str]):
        self.kappas = kappas
        self.couplings = np.empty([0, len(kappas)])

    def append(self, couplings: np.ndarray):
        couplings = np.asarray(couplings)
        if couplings.shape[-1] != len(self.kappas):
            raise CouplingError(
                f"coupling shape {couplings.shape} does not match (, {len(self.kappas)})"
            )
        self.couplings = np.concatenate((self.couplings, couplings), axis=0)
        return self

    def meshgrid(self, default: float = 1, **kwargs):
        self.append(
            np.stack(
                np.meshgrid(*[kwargs.get(kappa, default) for kappa in self.kappas]),
                axis=-1,
            ).reshape([-1, len(self.kappas)])
        )
        return self

    def reshape(self, kappas: list[str], default: float = 1):
        if kappas == self.kappas:
            return self
        new = Coupling(kappas)
        couplings = []
        for kappa in kappas:
            try:
                idx = self.kappas.index(kappa)
                couplings.append(self.couplings[:, idx : idx + 1])
            except ValueError:
                couplings.append(np.tile(default, [self.couplings.shape[0], 1]))
        new.couplings = np.concatenate(couplings, axis=-1)
        return new

    def __iadd__(self, other: Coupling) -> Coupling:
        if isinstance(other, Coupling):
            self.append(other.reshape(self.kappas).couplings)
            return self
        return NotImplemented

    def __len__(self):
        return len(self.couplings)

    def __getitem__(self, idx):
        return dict[str, npt.NDArray](zip(self.kappas, self.couplings[idx].T))

    def __iter__(self) -> Generator[dict[str, float]]:
        for i in range(len(self)):
            yield self[i]


class Diagram(ABC):
    @abstractproperty
    def diagrams(self) -> tuple[list[str], list]: ...

    def __init__(self, basis, unit_basis_weight=True):
        self.unit_basis_weight = unit_basis_weight
        basis = np.asarray(basis)
        count = len(self.diagrams[0])
        self._basis = Coupling(self.diagrams[0]).append(basis[:, :count])
        self._intm2 = basis[:, count]
        self._intm2_unc = (
            basis[:, count + 1] if basis.shape[1] >= (count + 2) else self._intm2
        )
        self._transmat = np.linalg.pinv(self._component_scale(self._basis.couplings))
        _s = self._transmat.shape
        if _s[1] < _s[0]:
            raise CouplingError(f"require more couplings ({_s[1]}/{_s[0]} provided)")

    def _component_scale(self, couplings):
        couplings = np.asarray(couplings)[:, :, np.newaxis]
        diagram = np.asarray(self.diagrams[1]).T[np.newaxis, :, :]
        idx2 = np.stack(np.tril_indices(diagram.shape[-1]), axis=-1)
        diagram2 = np.unique(np.sum(diagram[:, :, idx2], axis=-1), axis=-1)
        return np.product(np.power(couplings, diagram2), axis=1)

    def weight(self, couplings: Coupling):
        couplings = couplings.reshape(self.diagrams[0]).couplings
        weight = self._component_scale(couplings) @ self._transmat
        if self.unit_basis_weight:
            matched_basis = (
                (couplings == self._basis.couplings[:, np.newaxis]).all(-1).T
            )
            is_basis = matched_basis.any(-1)
            weight[is_basis] = matched_basis[is_basis]
        return weight

    def _sumw_intm2(self, couplings: Coupling):
        return unpack(self.weight(couplings) @ self._intm2)

    def _sqrt_sumw2_intm2_unc(self, couplings: Coupling):
        return unpack(np.sqrt(self.weight(couplings) ** 2 @ self._intm2_unc**2))


class Decay:
    _decays: dict[str, dict[str, FormulaBR | float]] = {}
    _widths: dict[str, float] = {}

    @staticmethod
    def parent(decay: str):
        return decay.split("->")[0]

    @staticmethod
    def width(particle: str, coupling: Coupling = ...):
        """[GeV]"""
        if coupling is ...:
            return Decay._widths[particle]
        else:
            decays = [
                decay
                for decay in Decay._decays[particle].values()
                if isinstance(decay, FormulaBR) and decay.total
            ]
            return (
                Decay._widths[particle]
                - np.sum(
                    [
                        decay.width(Coupling(decay.diagrams[0]).meshgrid())
                        for decay in decays
                    ]
                )
            ) + np.sum([decay.width(coupling) for decay in decays], axis=0)

    @classmethod
    def add(cls, decay: FormulaBR | str, br: float = None, width: float = None):
        if isinstance(decay, FormulaBR):
            br = decay
            decay = decay.decay
        else:
            if "->" not in decay:
                if width is None:
                    raise CouplingError(
                        f'total width of particle "{decay}" must be provided'
                    )
                br = None
                cls._widths[decay] = width
            else:
                if br is None:
                    if width is None:
                        raise CouplingError(
                            f'either BR or decay width of "{decay}" must be provided'
                        )
                    br = width / cls.width(cls.parent(decay))
        if br is not None:
            cls._decays.setdefault(cls.parent(decay), {})[decay] = br

    @classmethod
    def br(cls, decay: str, process: str) -> float:
        decay = Decay(decay)
        if isinstance(decay, FormulaBR):
            return decay(process)
        else:
            return decay

    def __new__(cls, decay: str):
        try:
            return cls._decays[cls.parent(decay)][decay]
        except:
            raise CouplingError(f"BR({decay}) is not recorded")


class Formula(ABC):
    @abstractproperty
    def search_pattern(self) -> re.Pattern: ...

    format_pattern: str = None
    number_pattern: str = "{:.1f}"
    number_separator: str = "_"

    @property
    def _kappas(self):
        return [key[1] for key in Formatter().parse(self.format_pattern)]

    def match(self, process: str):
        return re.match(self.search_pattern, process) is not None

    def parameters(self, process: str) -> dict[str, float]:
        pars = {}
        for group in re.finditer(self.search_pattern, process):
            for k, v in group.groupdict().items():
                if v is not None:
                    pars[k] = self._parse_number(v)
        return pars

    @overload
    def _parse_number(self, value: str) -> float: ...
    @overload
    def _parse_number(self, value: float) -> str: ...
    def _parse_number(self, value):
        if isinstance(value, str):
            return float(value.replace(self.number_separator, "."))
        else:
            return self.number_pattern.format(value).replace(".", self.number_separator)

    def process(self, couplings: Coupling):
        return [
            self.format_pattern.format(
                **{k: self._parse_number(v) for k, v in coupling.items()}
            )
            for coupling in couplings.reshape(self._kappas)
        ]

    @abstractmethod
    def __call__(self, process: str): ...


class FormulaXS(Diagram, Formula):
    def __init__(self, basis_xs, unit_basis_weight=True):
        super().__init__(basis_xs, unit_basis_weight)

    def xs(self, couplings: Coupling):
        """[pb]"""
        return self._sumw_intm2(couplings)

    def xs_unc(self, couplings: Coupling):
        """[pb]"""
        return self._sqrt_sumw2_intm2_unc(couplings)

    def __call__(self, process: str):
        return self.xs(Coupling(self.diagrams[0]).meshgrid(**self.parameters(process)))

    @cached_property
    def basis_process(self):
        return self.process(self._basis)


class FormulaBR(Diagram, Formula):
    def __init__(
        self,
        decay: str,
        basis_br=None,
        basis_width=None,
        total=False,
        unit_basis_weight=True,
    ):
        self.decay = decay
        self.parent = Decay.parent(decay)
        self.total = total
        if basis_br is not None:
            basis = np.asarray(basis_br)
            basis[:, len(self.diagrams[0])] = basis[
                :, len(self.diagrams[0])
            ] * Decay.width(self.parent)
        elif basis_width is not None:
            basis = np.asarray(basis_width)
        else:
            raise CouplingError(
                f'either BR or decay width of "{decay}" must be provided'
            )
        super().__init__(basis, unit_basis_weight)

    def width(self, couplings: Coupling):
        """[GeV]"""
        return self._sumw_intm2(couplings)

    def br(self, couplings: Coupling):
        return self.width(couplings) / Decay.width(self.parent, couplings)

    def __call__(self, process: str):
        return self.br(Coupling(self.diagrams[0]).meshgrid(**self.parameters(process)))
