from __future__ import annotations

import re
from abc import ABC, abstractmethod

import numpy as np

class FormulaError(Exception):
    __module__ = Exception.__module__

class Formula(ABC):
    _pattern = None
    _diagram = None

    def _scale_xs_components(self, couplings):
        couplings  = np.asarray(couplings) [:, :, np.newaxis]
        diagrams   = np.asarray(self._diagram).T[np.newaxis, :, :]
        idx_2 = np.stack(np.tril_indices(diagrams.shape[-1]), axis = -1)
        diagrams_2 = np.unique(np.sum(diagrams[:, :, idx_2], axis = -1), axis = -1)
        return np.product(np.power(couplings, diagrams_2), axis = 1)

    def __init__(self, *basis, unit_basis_weight = True):
        assert self._pattern is not None and self._diagram is not None
        self.unit_basis_weight = unit_basis_weight
        basis = np.asarray(basis)
        self.basis_coupling  = basis[:, :-1]
        self.basis_xs        = basis[:,  -1]
        self.transmat        = np.linalg.pinv(self._scale_xs_components(self.basis_coupling))
        _s = self.transmat.shape
        if _s[1] < _s[0]:
            raise FormulaError(f'require at least {_s[0] - _s[1]} more coupling combinations')

    def match(self, process: str):
        return re.match(self._pattern, process) is not None

    def get_parameter(self, process: str):
        pars = re.search(self._pattern, process)
        if pars:
            return dict((key, self.get_value(value)) for key, value in pars.groupdict().items())

    def __call__(self, process: str):
        parameters = self.get_parameter(process)
        if parameters:
            return self.get_xs(**parameters)

    @abstractmethod
    def get_weight(self, *args, **kwargs):
        ...
    def _get_weight(self, couplings):
        weight = self._scale_xs_components(couplings) @ self.transmat
        if self.unit_basis_weight:
            matched_basis = (couplings == self.basis_coupling[:, np.newaxis]).all(-1).T
            is_basis = matched_basis.any(-1)
            weight[is_basis] = matched_basis[is_basis]
        return weight

    @abstractmethod
    def get_xs(self, *args, **kwargs):
        ...
    def _get_xs(self, *args, **kwargs):
        xs = self.get_weight(*args, **kwargs) @ self.basis_xs
        if xs.shape[0] == 1:
            return xs[0]
        return xs

    @abstractmethod
    def get_value(self, text: str) -> float:
        ...