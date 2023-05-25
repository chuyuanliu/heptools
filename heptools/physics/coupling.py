'''
    kappa-framework
'''

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from functools import cached_property
from string import Formatter

import numpy as np


class CouplingError(Exception):
    __module__ = Exception.__module__

class Coupling:
    def __init__(self, kappas: list[str]):
        self.kappas = kappas
        self.couplings = np.empty([0, len(kappas)])

    def append(self, couplings: np.ndarray):
        if couplings.shape[-1] != len(self.kappas):
            raise CouplingError(f'coupling shape {couplings.shape} does not match (, {len(self.kappas)})')
        self.couplings = np.concatenate((self.couplings, couplings), axis = 0)
        return self

    def meshgrid(self, default: float = 1, **kwargs):
        self.append(np.stack(np.meshgrid(*[kwargs.get(kappa, default) for kappa in self.kappas]), axis = -1).reshape([-1, len(self.kappas)]))
        return self

    def reshape(self, kappas: list[str], default: float = 1):
        if kappas == self.kappas:
            return self
        new = Coupling(kappas)
        couplings = []
        for kappa in kappas:
            try:
                idx = self.kappas.index(kappa)
                couplings.append(self.couplings[:, idx: idx + 1])
            except ValueError:
                couplings.append(np.tile(default, [self.couplings.shape[0], 1]))                
        new.couplings = np.concatenate(couplings, axis = -1)
        return new

    def __iadd__(self, other: Coupling):
        self.append(other.reshape(self.kappas).couplings)
        return self

    def __len__(self):
        return len(self.couplings)

    def __getitem__(self, idx):
        return dict(zip(self.kappas, self.couplings[idx].T))

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

class Diagram:
    _diagram: tuple[list[str], list] = None

    def __init__(self, basis, unit_basis_weight = True, diagram: tuple[list[str], list] = None):
        if diagram is not None:
            self._diagram = diagram
        assert self._diagram is not None
        self.unit_basis_weight = unit_basis_weight
        basis = np.asarray(basis)
        self._basis = Coupling(self._diagram[0]).append(basis[:, :-1])
        self._int_m2 = basis[:, -1]
        self._transmat = np.linalg.pinv(self.scale_m2(self._basis.couplings))
        _s = self._transmat.shape
        if _s[1] < _s[0]:
            raise CouplingError(f'require at least {_s[0] - _s[1]} more coupling combinations')

    def scale_m2(self, couplings):
        couplings = np.asarray(couplings)[:, :, np.newaxis]
        diagrams  = np.asarray(self._diagram[1]).T[np.newaxis, :, :]
        idx2 = np.stack(np.tril_indices(diagrams.shape[-1]), axis = -1)
        diagram2  = np.unique(np.sum(diagrams[:, :, idx2], axis = -1), axis = -1)
        return np.product(np.power(couplings, diagram2), axis = 1)

    def weight(self, couplings: Coupling):
        couplings = couplings.reshape(self._diagram[0]).couplings
        weight = self.scale_m2(couplings) @ self._transmat
        if self.unit_basis_weight:
            matched_basis = (couplings == self._basis.couplings[:, np.newaxis]).all(-1).T
            is_basis = matched_basis.any(-1)
            weight[is_basis] = matched_basis[is_basis]
        return weight

    def int_m2(self, couplings: Coupling):
        int_m2 = self.weight(couplings) @ self._int_m2
        if int_m2.shape[0] == 1:
            return int_m2[0]
        return int_m2

class Decay:
    _decays: dict[str, dict[str, FormulaBR | float]] = {}
    _widths: dict[str, float] = {}

    @staticmethod
    def parent(decay: str):
        return decay.split('->')[0]

    @staticmethod
    def width(particle: str, coupling: Coupling = None):
        '''[GeV]'''
        if coupling is None:
            return Decay._widths[particle]
        else:
            decays = [decay for decay in Decay._decays[particle].values() if isinstance(decay, FormulaBR) and decay.total]
            return (Decay._widths[particle] - np.sum([decay.width(Coupling(decay._diagram[0]).meshgrid()) for decay in decays])) + np.sum([decay.width(coupling) for decay in decays], axis = 0)

    @classmethod
    def add(cls, decay: FormulaBR | str, br: float = None, width: float = None):
        if isinstance(decay, FormulaBR):
            br    = decay
            decay = decay.decay
        else:
            if '->' not in decay:
                assert width is not None
                br = None
                cls._widths[decay] = width
            else:
                if br is None:
                    if width is None:
                        raise CouplingError(f'either BR or decay width of ({decay}) must be specified')
                    br = width/cls.width(cls.parent(decay))
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
            raise CouplingError(f'BR({decay}) is not recorded')

class Formula(ABC):
    _pattern: str = None
    _parameters_pattern: str | re.Pattern = None

    def __init__(self, pattern: str = None):
        if pattern is not None:
            self.pattern = pattern
        assert self.pattern is not None

    @property
    def pattern(self):
        return self._pattern

    @pattern.setter
    def pattern(self, pattern):
        self._pattern = pattern
        for k in ['_re_pattern', '_kappas']:
            if k in self.__dict__:
                del self.__dict__[k]

    @cached_property
    def _re_pattern(self) -> re.Pattern:
        return re.compile(self.pattern.format(**dict((kappa, f'(?P<{kappa}>.+)') for kappa in self._kappas)) )

    @cached_property
    def _kappas(self):
        return [key[1] for key in Formatter().parse(self.pattern)]

    def match(self, process: str):
        return re.match(self._re_pattern, process) is not None

    def parameters(self, process: str) -> dict[str, float]:
        if self._parameters_pattern is not None:
            _pattern = self._parameters_pattern
        else:
            _pattern = self._re_pattern
        pars = {}
        for group in re.finditer(_pattern, process):
            for k, v in group.groupdict().items():
                if v is not None:
                    pars[k] = self._parse_number(v)
        return pars

    _decimal_separator: str = None
    _decimal_pattern  : str = None

    def _parse_number(self, value: str | float | int):
        if isinstance(value, str):
            return float(value.replace(self._decimal_separator, '.'))
        else:
            return self._decimal_pattern.format(value).replace('.', self._decimal_separator)

    def process(self, couplings: Coupling):
        return [self.pattern.format(**coupling) for coupling in couplings.reshape(self._kappas)]

    @abstractmethod
    def __call__(self, process: str):
        ...

class FormulaXS(Diagram, Formula):
    def __init__(self, basis_xs, unit_basis_weight = True, diagram: tuple[list[str], list] = None, pattern: str = None):
        Diagram.__init__(self, basis_xs, unit_basis_weight, diagram)
        Formula.__init__(self, pattern)

    def xs(self, couplings: Coupling):
        '''[pb]'''
        return self.int_m2(couplings)

    def __call__(self, process: str):
        return self.xs(Coupling(self._diagram[0]).meshgrid(**self.parameters(process)))

    @cached_property
    def basis_process(self):
        return self.process(self._basis)

class FormulaBR(Diagram, Formula):
    def __init__(self, decay: str, basis_br = None, basis_width = None, total = False, unit_basis_weight = True, diagram: tuple[list[str], list] = None, pattern: str = None):
        self.decay  = decay
        self.parent = Decay.parent(decay)
        self.total  = total
        if basis_br is not None:
            basis = np.asarray(basis_br)
            basis[:, -1] = basis[:, -1] * Decay.width(self.parent)
        elif basis_width is not None:
            basis = np.asarray(basis_width)
        else:
            raise CouplingError(f'either BR or decay width of ({decay}) must be specified')
        Diagram.__init__(self, basis, unit_basis_weight, diagram)
        Formula.__init__(self, pattern)

    def width(self, couplings: Coupling):
        '''[GeV]'''
        return self.int_m2(couplings)

    def br(self, couplings: Coupling):
        return self.width(couplings) / Decay.width(self.parent, couplings)

    def __call__(self, process: str):
        return self.br(Coupling(self._diagram[0]).meshgrid(**self.parameters(process)))