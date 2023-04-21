from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar

from .coupling import Formula


class XSectionError(Exception):
    __module__ = Exception.__module__

@dataclass
class XSection:
    '''
        [pb]
        - PDG https://pdg.lbl.gov/
        - GenXsecAnalyzer https://twiki.cern.ch/twiki/bin/viewauth/CMS/HowToGenXSecAnalyzer
    '''
    BRs : ClassVar[dict[str, float]] = {}
    _all: ClassVar[list[ XSection ]] = []

    process: Formula | str
    xs: float | str
    decay: str
    kfactors: dict[str, float]

    @classmethod
    def _get_br(cls, decay: str) -> float:
        if not decay:
            return 1
        for k in cls.BRs:
            decay = re.sub(rf'(?<!\w){k}(?!\w)', rf'cls.BRs["{k}"]', decay)
        return eval(decay)

    @classmethod
    def _get_xs(cls, xs: str) -> float:
        if not xs:
            return 0
        xs = re.sub(r'\[(?P<process>[\w]+)\]', r'cls("\g<process>")', xs)
        xs = re.sub(r'\[(?P<process>[\w",]+)\]', r'cls(\g<process>)', xs)
        return eval(xs)

    @classmethod
    def add(cls, process: Formula | str, xs: float | str = None, decay: str = '', kfactors: dict[str, float] = None):
        self = object.__new__(cls)
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
        if isinstance(self.process, str):
            match = re.match(self.process, process)
            if match:
                xs = self.xs
                if isinstance(xs, str):
                    xs = self._get_xs(xs.format(**match.groupdict()))
        elif isinstance(self.process, Formula):
            if self.process.match(process):
                xs = self.process(process)
        if xs:
            xs *= XSection._get_br(self.decay if decay is None else decay)
            if self.kfactors and kfactors:
                for kfactor in kfactors:
                    xs *= self.kfactors.get(kfactor, 1)
        return xs