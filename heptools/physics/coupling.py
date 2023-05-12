from __future__ import annotations

import re

import numpy as np


class FormulaError(Exception):
    __module__ = Exception.__module__

class Formula:
    _pattern = None
    _diagram = None

    @classmethod
    @property
    def _re_pattern(cls):
        return re.compile(re.sub(r'\[(?P<coupling>[\w]+)\]', r'(?P<\g<coupling>>.+)', cls._pattern))

    @classmethod
    @property
    def _format_pattern(cls):
        return re.sub(r'\[(?P<coupling>[\w]+)\]', r'{\g<coupling>}', cls._pattern)

    @classmethod
    def _scale_xs_components(cls, couplings):
        couplings  = np.asarray(couplings) [:, :, np.newaxis]
        diagrams   = np.asarray(cls._diagram[1]).T[np.newaxis, :, :]
        idx_2 = np.stack(np.tril_indices(diagrams.shape[-1]), axis = -1)
        diagrams_2 = np.unique(np.sum(diagrams[:, :, idx_2], axis = -1), axis = -1)
        return np.product(np.power(couplings, diagrams_2), axis = 1)

    @classmethod
    def _expand(cls, couplings = None, **kwargs):
        if couplings is not None:
            return np.asarray(couplings)
        else:
            return np.stack(np.meshgrid(*[kwargs.get(_c, 1) for _c in cls._diagram[0]]), axis=-1).reshape([-1,len(cls._diagram[0])])

    @classmethod
    def _parameter(cls, process: str):
        pars = re.search(cls._re_pattern, process)
        if pars:
            return dict((key, cls._parse_number(pars.groupdict()[key])) for key in cls._diagram[0])
        else:
            raise FormulaError(f'cannot extract parameters from "{process}" using "{cls._pattern}')

    @classmethod
    def match(cls, process: str):
        return re.match(cls._re_pattern, process) is not None

    @classmethod
    def process(cls, couplings = None, **kwargs):
        couplings = cls._expand(couplings, **kwargs)
        for coupling in couplings:
            yield cls._format_pattern.format(**dict(zip(cls._diagram[0], [cls._parse_number(_c) for _c in coupling])))

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

    def __call__(self, process: str):
        parameters = self._parameter(process)
        if parameters:
            return self.xs(**parameters)

    @property
    def basis_process(self):
        return self.process(couplings = self.basis_coupling)

    def weight(self, couplings = None, **kwargs):
        couplings = self._expand(couplings, **kwargs)
        weight = self._scale_xs_components(couplings) @ self.transmat
        if self.unit_basis_weight:
            matched_basis = (couplings == self.basis_coupling[:, np.newaxis]).all(-1).T
            is_basis = matched_basis.any(-1)
            weight[is_basis] = matched_basis[is_basis]
        return weight

    def xs(self, couplings = None, **kwargs):
        xs = self.weight(couplings, **kwargs) @ self.basis_xs
        if xs.shape[0] == 1:
            return xs[0]
        return xs

    _decimal_separator = None
    _decimal_pattern   = None

    @classmethod
    def _parse_number(cls, value):
        if isinstance(value, str):
            return float(value.replace(cls._decimal_separator, '.'))
        else:
            return cls._decimal_pattern.format(value).replace('.', cls._decimal_separator)