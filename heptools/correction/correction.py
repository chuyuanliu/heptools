from __future__ import annotations

from functools import partial
from typing import Callable, Iterable

import awkward as ak
from correctionlib import CorrectionSet

from ..aktools import mul_arrays
from .weight import ContentLike, _get_content


class CorrectionError(Exception):
    __module__ = Exception.__module__

class _Correction:
    def __init__(self, file: str = None):
        if file is not None:
            self.corrections = CorrectionSet.from_file(file)
        else:
            raise CorrectionError()

    def _evaluate(self, events: ak.Array, _correction: str = None, **inputs: ContentLike):
        if _correction is None:
            assert len(self.corrections) == 1
            _correction = next(iter(self.corrections))
        corr, args = self.corrections[_correction], []
        for var in corr.inputs:
            arg = inputs.get(var.name, var.name)
            args.append(arg if var.type == 'string' and isinstance(arg, str) else _get_content(events, arg))
        return corr.evaluate(*args)

    def __str__(self):
        vline_l, vline_m = '-'*30, '-'*10
        lines = [vline_l]
        for k, v in self.corrections.items():
            lines += ([f'[{k}]']
                    + ([v.description] if v.description else [])
                    + [vline_m]
                    + [f'- {i.type:<7}{i.name:<20} {i.description}' for i in v.inputs]
                    + [vline_l])
        return '\n'.join(lines)

class EventLevelCorrection(_Correction):
    def evaluate(self, _correction: str = None, **inputs: ContentLike):
        return partial(self._evaluate, _correction = _correction, **inputs)

class ObjectLevelCorrection(_Correction):
    def _evaluate_objects(self, events: ak.Array, _selection: Callable[[ak.Array], ak.Array] = None, _transform: Callable[[ak.Array, ak.Array], ak.Array] = None, _correction: str = None, **inputs: ContentLike) -> ak.Array:
        if _selection is not None:
            events = _selection(events)
        corrections = ak.unflatten(self._evaluate(ak.flatten(events), _correction, **inputs), counts = ak.num(events))
        if _transform is not None:
            corrections = _transform(events, corrections)
        return corrections

    def _evaluate_events(self, events: ak.Array, **kwargs) -> ak.Array:
        return ak.prod(self._evaluate_objects(events, **kwargs), axis = 1)

    def _evaluate_groups(self, events: ak.Array, groups: Iterable[tuple[Callable[[ak.Array], ak.Array], dict[str, ContentLike]]], **kwargs):
        return mul_arrays(*[self._evaluate_events(events, **(kwargs | {'_selection': group[0]} | group[1])) for group in groups])

    def evaluate(self, _correction: str = None, *groups: tuple[Callable[[ak.Array], ak.Array], dict[str, ContentLike]], **inputs: ContentLike):
        if groups:
            return partial(self._evaluate_groups, groups = groups, _correction = _correction, **inputs)
        else:
            return partial(self._evaluate_events, _correction = _correction, **inputs)