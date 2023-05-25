from functools import partial
from numbers import Number
from typing import Callable, Union

import awkward as ak
import numpy as np

from .._utils import isinstance_
from ..aktools import FieldLike, get_field, mul_arrays

ContentLike = Union[FieldLike, Callable, Number]

def _get_content(data: ak.Array, content: ContentLike):
    if isinstance_(content, FieldLike):
        return get_field(data, content)
    elif isinstance(content, Callable):
        return content(data)
    return content

class EventWeight:
    def __init__(self):
        self.weights     : dict[str, dict[str,  ContentLike  ]] = {}
        self.correlations: dict[str, dict[str, dict[str, str]]] = {}

    def add(self, name: str, central: ContentLike = ..., **variations: ContentLike):
        if central is ...:
            assert '' in variations
        self.weights[name] = {'': central} | variations

    def correlate(self, name: str, **variations: dict[str, str]):
        self.correlations[name] = variations

    def __iter__(self):
        return self.variations()

    def variations(self, *exclude: str):
        exclude = set(exclude)
        for wsk, wsv in self.weights.items():
            if wsk not in exclude:
                for wv in wsv:
                    if wv:
                        yield f'{wsk}.{wv}'
        for csk, csv in self.correlations.items():
            if csk not in exclude:
                for ck, cv in csv.items():
                    if not ({*cv} & exclude):
                        yield f'{csk}.{ck}'

    def _multiply_weights(self, weights: dict[str, dict[str]], size: int, **variations: str):
        _numbers, _arrays = 1, []
        for wsk, wsv in weights.items():
            v = wsv[variations.get(wsk, '')]
            if isinstance(v, Number):
                _numbers *= v
            else:
                _arrays.append(v)
        if len(_arrays) == 0:
            return np.repeat(_numbers, size)
        else:
            _array = mul_arrays(*_arrays)
            return _array * _numbers if _numbers != 1 else _array

    def __call__(self, events: ak.Array, *exclude: str) -> ak.Array:
        exclude = set(exclude)
        ws: dict[str,  dict[str]] = {}
        for wsk, wsv in self.weights.items():
            if wsk not in exclude:
                ws[wsk] = {wk: _get_content(events, wv) for wk, wv in wsv.items()}
        zeros   = {wsk for wsk in ws if np.any(ws[wsk][''] == 0)}
        mul_ws  = partial(self._multiply_weights, ws, len(events))
        weights = ak.Array({'weight': mul_ws()})
        for wsk, wsv in ws.items():
            if len(wsv) > 1:
                weight = ak.Array({})
                for wk, wv in wsv.items():
                    if wk:
                        weight[wk] = mul_ws(**{wsk: wk}) if wsk in zeros else weights['weight'] * (wv / wsv[''])
                weights[wsk] = weight
        for csk, csv in self.correlations.items():
            if csk not in exclude:
                weight, empty = ak.Array({}), True
                for ck, cv in csv.items():
                    if not ({*cv} & exclude):
                        empty = False
                        weight[ck] = mul_ws(**cv) if any([k in zeros for k in cv]) else weights['weight'] * mul_arrays(*[ws[k][v] / ws[k][''] for k, v in cv.items()])
                if not empty:
                    weights[csk] = weight
        return weights