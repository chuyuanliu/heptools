'''
    kappa-framework
'''

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from string import Formatter

import numpy as np


class FormulaError(Exception):
    __module__ = Exception.__module__

class Coupling:
    def __init__(self, kappas: list[str]):
        self.kappas = kappas
        self.couplings = np.empty([0, len(kappas)])

    def append(self, couplings: np.ndarray):
        if couplings.shape[-1] != len(self.kappas):
            raise FormulaError(f'couplings shape {couplings.shape} does not match (, {len(self.kappas)})')
        self.couplings = np.concatenate((self.couplings, couplings), axis = 0)
        return self

    def meshgrid(self, default: float = 1, **kwargs):
        self.append(np.stack(np.meshgrid(*[kwargs.get(kappa, default) for kappa in self.kappas]), axis=-1).reshape([-1, len(self.kappas)]))
        return self

    def reshape(self, kappas: list[str], default: float = 1):
        new = Coupling(kappas)
        couplings = []
        for kappa in kappas:
            try:
                idx = self.kappas.index(kappa)
                couplings.append(self.couplings[:, idx: idx + 1])
            except ValueError:
                couplings.append(np.repeat(default, [self.couplings.shape[0], 1]))                
        new.couplings = np.concatenate(couplings, axis = -1)
        return new

    def __iadd__(self, other: Coupling):
        self.append(other.reshape(self.kappas).couplings)
        return self

    def __iter__(self):
        return iter(self.couplings)

    def __len__(self):
        return len(self.couplings)

    def __getitem__(self, idx):
        return dict(zip(self.kappas, self.couplings[idx]))

class Formula(ABC):
    _pattern: str = None

    @classmethod
    @property
    def _kappas(cls) -> list[str]:
        return [key[1] for key in Formatter().parse(cls._pattern)]

    @classmethod
    @property
    def _re_pattern(cls) -> str:
        return cls._pattern.format(**dict((kappa, f'(?P<{kappa}>.+)') for kappa in cls._kappas))

    @classmethod
    def match(cls, process: str):
        return re.match(cls._re_pattern, process) is not None

    @classmethod
    def search(cls, process: str):
        return re.search(cls._re_pattern, process) is not None

    @classmethod
    def couplings(cls, process: str) -> dict[str, float]:
        pars = re.search(cls._re_pattern, process)
        if pars:
            return dict((k, cls._parse_number(v)) for k, v in pars.groupdict().items())
        else:
            raise FormulaError(f'cannot extract couplings from "{process}" using "{cls._pattern}')

    _decimal_separator: str = None
    _decimal_pattern  : str = None

    @classmethod
    def _parse_number(cls, value: str | float | int):
        if isinstance(value, str):
            return float(value.replace(cls._decimal_separator, '.'))
        else:
            return cls._decimal_pattern.format(value).replace('.', cls._decimal_separator)

    @classmethod
    def process(cls, couplings: Coupling):
        return [cls._pattern.format(**dict(zip(cls._kappas, [cls._parse_number(_c) for _c in coupling]))) for coupling in couplings.reshape(cls._kappas)]

    @abstractmethod
    def __call__(self, process: str):
        ...

class FormulaXS(Formula):
    _diagram      = None

    @classmethod
    def _scale_xs_components(cls, couplings):
        couplings = np.asarray(couplings) [:, :, np.newaxis]
        diagrams  = np.asarray(cls._diagram[1]).T[np.newaxis, :, :]
        idx_2 = np.stack(np.tril_indices(diagrams.shape[-1]), axis = -1)
        diagrams_2 = np.unique(np.sum(diagrams[:, :, idx_2], axis = -1), axis = -1)
        return np.product(np.power(couplings, diagrams_2), axis = 1)

    def __init__(self, *basis, unit_basis_weight = True):
        assert self._pattern is not None and self._diagram is not None
        self.unit_basis_weight = unit_basis_weight
        basis = np.asarray(basis)
        self.basis    = Coupling(self._diagram[0]).append(basis[:, :-1])
        self.basis_xs = basis[:, -1]
        self.transmat = np.linalg.pinv(self._scale_xs_components(self.basis.couplings))
        _s = self.transmat.shape
        if _s[1] < _s[0]:
            raise FormulaError(f'require at least {_s[0] - _s[1]} more coupling combinations')

    def __call__(self, process: str):
        parameters = self.couplings(process)
        if parameters:
            return self.xs(Coupling(self._diagram[0]).meshgrid(**parameters))

    def weight(self, couplings: Coupling):
        couplings = couplings.reshape(self._diagram[0]).couplings
        weight = self._scale_xs_components(couplings) @ self.transmat
        if self.unit_basis_weight:
            matched_basis = (couplings == self.basis.couplings[:, np.newaxis]).all(-1).T
            is_basis = matched_basis.any(-1)
            weight[is_basis] = matched_basis[is_basis]
        return weight

    def xs(self, couplings: Coupling):
        xs = self.weight(couplings) @ self.basis_xs
        if xs.shape[0] == 1:
            return xs[0]
        return xs

    @property
    def basis_process(self):
        return self.process(self.basis)

class FormulaBR(Formula):
    ... #TODO