from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar

from ..utils import Eval
from .coupling import Decay, Formula


class XSectionError(Exception):
    __module__ = Exception.__module__

@dataclass
class XSection:
    '''
        [pb]
        - PDG https://pdg.lbl.gov/
        - GenXsecAnalyzer https://twiki.cern.ch/twiki/bin/viewauth/CMS/HowToGenXSecAnalyzer
    '''
    _all: ClassVar[list[XSection]] = []

    process: Formula | re.Pattern
    xs: float | str
    decay: str
    kfactors: dict[str, float]

    @classmethod
    def _get_br(cls, decay: str, process: str) -> float:
        if not decay:
            return 1
        return Eval(Decay.br, process)(decay)

    @classmethod
    def _get_xs(cls, xs: str) -> float:
        if not xs:
            return 0
        return Eval(cls)(xs)

    @classmethod
    def add(cls, process: Formula | re.Pattern | str, xs: float | str = None, decay: str = '', kfactors: dict[str, float] = None):
        self = object.__new__(cls)
        if isinstance(process, str):
            process = re.compile(process)
        self.__init__(process = process, xs = xs, decay = decay, kfactors = kfactors)
        cls._all.append(self)

    def __new__(cls, process: str, *kfactors: str, decay: str = None):
        for _xs in cls._all:
            xs = _xs(process, decay, kfactors)
            if xs is not None:
                return xs
        raise XSectionError(f'the cross section of "{process}" is not recorded')

    def __call__(self, process: str, decay: str = None, kfactors: list[str] | str = None) -> float:
        xs = None
        match = self.process.match(process)
        if match:
            xs = self.xs
            if xs is None:
                xs = self.process(process)
            elif isinstance(xs, str):
                xs = self._get_xs(xs.format(**match.groupdict()))
        if xs:
            xs *= XSection._get_br(decay = self.decay if decay is None else decay, process = process)
            if self.kfactors and kfactors:
                for kfactor in kfactors:
                    xs *= self.kfactors.get(kfactor, 1)
        return xs