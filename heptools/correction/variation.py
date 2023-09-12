from abc import ABC, abstractmethod
from functools import partial

import numpy as np

from ..utils import arg_new, sequence_call
from .correction import (CorrectionError, EventLevelCorrection,
                         ObjectLevelCorrection, _Correction)


class _Variation(_Correction, ABC):
    @abstractmethod
    def _default(self) -> list[str]:
        ...

    @abstractmethod
    def _corrections(self) -> dict[str]:
        ...

    def __init__(self, file: str, variations: list[str] = ...):
        self.variations = set(arg_new(variations, list, self._default) + [''])
        super().__init__(file)

    def __new__(cls, *args, **kwargs):
        self = object().__new__(cls)
        try:
            self.__init__(*args, **kwargs)
        except CorrectionError:
            return {'': 1}
        return self._corrections()

class PileupWeight(_Variation, EventLevelCorrection):
    _names = {'': 'nominal'}

    def _default(self):
        return ['up', 'down']

    def _corrections(self):
        return {k: self.evaluate(weights = self._names.get(k, k), NumTrueInteractions = ('Pileup', 'nTrueInt')) for k in self.variations}

class BTagSF_Shape(_Variation, ObjectLevelCorrection):
    '''
        - https://twiki.cern.ch/twiki/bin/view/CMS/BTagShapeCalibration
    '''
    _names = {'': 'central'}

    def __init__(self, file: str, variations: list[str] = ..., jet: str = 'Jet', unbounded: bool = False, jes: list[str] = None):
        self.jes = [] if jes is None else jes
        self.target = (lambda x: x[jet],)
        if unbounded:
            self.target += (lambda x: x[np.abs(x.eta) < 2.5],)
        super().__init__(file, variations)

    def _default(self):
        return [f'{direction}_{source}' for source in ['lf', 'lfstats1', 'lfstats2', 'hf', 'hfstats1', 'hfstats2', 'cferr1', 'cferr2']
                + [f'jes{jes}' for jes in self.jes] for direction in ['up', 'down']]

    def _corrections(self):
        groups = []
        flavor_gudsb = sequence_call(*self.target, lambda x: x[x.hadronFlavour != 4])
        flavor_c     = sequence_call(*self.target, lambda x: x[x.hadronFlavour == 4])
        for var in self.variations:
            if 'cferr' in var:
                groups.append((var, (flavor_gudsb, {}), (flavor_c, {'systematic': self._names.get(var, var)})))
            else:
                groups.append((var, (flavor_gudsb, {'systematic': self._names.get(var, var)})))
        return {group[0]: self.evaluate('deepJet_shape', *group[1:], systematic = 'central', flavor = 'hadronFlavour', abseta = lambda x: np.abs(x.eta), discriminant = 'btagDeepFlavB') for group in groups}

class PileupJetIDSF(_Variation, ObjectLevelCorrection):
    '''
        - https://twiki.cern.ch/twiki/bin/view/CMS/PileupJetIDUL#Data_MC_Efficiency_Scale_Factors
        - https://twiki.cern.ch/twiki/bin/view/CMS/BTagSFMethods#1a_Event_reweighting_using_scale
    '''
    _names = {0b000: '', 0b100: 'L', 0b110: 'M', 0b111: 'T', '': 'nom'}

    def __init__(self, file: str, variations: list[str] = ..., jet: str = 'Jet', working_point: int = 0, untagged: bool = False):
        self.working_point = working_point
        self.untagged = untagged
        self.target = (lambda x: x[jet], lambda x: x[(x.pt < 50) & (x.pt > 20) & (x.genJetIdx > -1)])
        super().__init__(file, variations)

    def _default(self):
        return ['up', 'down']

    def _corrections(self):
        wp  = self._names[self.working_point]
        if wp:
            MCEff = partial(self._evaluate_objects, _correction = 'PUJetID_eff', workingpoint = wp, systematic = 'MCEff')
            tagged   = sequence_call(*self.target, lambda x: x[x.puId >= self.working_point])
            untagged = sequence_call(*self.target, lambda x: x[x.puId <  self.working_point])
            group = [(tagged, {})]
            if self.untagged:
                def _transform(events, corrections):
                    eff = MCEff(events)
                    return (1 - corrections * eff)/(1 - eff)
                group.append((untagged, {'_transform': _transform}))
            return {k: self.evaluate('PUJetID_eff', *group, workingpoint = wp, systematic = self._names.get(k, k)) for k in self.variations}
        return {'': 1}