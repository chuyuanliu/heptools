from __future__ import annotations

from numbers import Number
from typing import Callable

import awkward as ak
import numpy as np
from hist.axis import Boolean, IntCategory, StrCategory

from ..aktools import AnyArray, FieldLike, and_fields, get_field
from ..typetools import isinstance_
from . import hist as hs

FillLike  = FieldLike | AnyArray | Number | bool | Callable

class FillError(Exception):
    __module__ = Exception.__module__

class Fill:
    threads = 1
    def __init__(self, fills: dict[str, list[str]] = None, weight = 'weight', **fill_args: FillLike):
        self._fills  = {} if fills is None else fills
        self._kwargs = fill_args | {'weight': weight}

    def __add__(self, other: Fill) -> Fill:
        if isinstance(other, Fill):
            fills  = other._fills  | self._fills
            kwargs = other._kwargs | self._kwargs
            return Fill(fills, **kwargs)
        else:
            return NotImplemented

    def __call__(self, events: ak.Array, hists: hs.Set = None, **fill_args: FillLike):
        self.fill(events, hists, **fill_args)

    def __setitem__(self, key: str, value: FillLike):
        self._kwargs[key] = value

    def __getitem__(self, key: str):
        return self._kwargs[key]

    def fill(self, events: ak.Array, hists: hs.Set = None, **fill_args: FillLike):
        if hists is None:
            if hs.Set.current is None:
                raise FillError('no histogram set is specified')
            hists  = hs.Set.current
        fill_args = self._kwargs | fill_args
        mask_categories = []
        for category in hists._categories:
            if category not in fill_args:
                if category in events.fields:
                    fill_args[category] = (category,)
                else:
                    if isinstance(hists._axes[category], StrCategory):
                        mask_categories.append(category)
                    else:
                        raise FillError(f'field "{category}" does not exist in data') from None
        for category_args in hists._generate_category_combinations(mask_categories):
            mask   = and_fields(events, *category_args.values())
            masked = events if mask is None else events[mask]
            for k, v in fill_args.items():
                if (isinstance(v, str) and k in hists._categories) or isinstance(v, (bool, Number)):
                    category_args[k] = v
                elif isinstance_(v, FieldLike):
                    category_args[k] = get_field(masked, v)
                elif isinstance_(v, AnyArray):
                    category_args[k] = v if mask is None else v[mask]
                elif isinstance_(v, Callable):
                    category_args[k] = v(masked)
                else:
                    raise FillError(f'cannot fill "{k}" with "{v}"')
            jagged_args = {}
            counts_args  = []
            for k, v in category_args.items():
                if isinstance(v, ak.Array):
                    try:
                        category_args[k] = ak.flatten(v)
                        count = ak.num(v)
                        for i, c in enumerate(counts_args):
                            if ak.all(c == count):
                                jagged_args[k] = i
                        if k not in jagged_args:
                            jagged_args[k] = len(counts_args)
                            counts_args.append(count)
                    except:
                        continue
            for name in self._fills:
                fills = {k: f'{name}:{k}' if f'{name}:{k}' in category_args else k for k in self._fills[name]}
                shape = {jagged_args[k] for k in fills.values() if k in jagged_args}
                if len(shape) == 0:
                    shape = None
                elif len(shape) == 1:
                    shape = counts_args[next(iter(shape))]
                else:
                    raise FillError(f'cannot fill hist "{name}" with unmatched jagged arrays')
                hist_args = {}
                for k, v in fills.items():
                    fill = category_args[v]
                    if shape is not None:
                        if v not in jagged_args and isinstance_(fill, AnyArray):
                            fill = np.repeat(fill, shape)
                    hist_args[k] = fill
                # https://github.com/scikit-hep/boost-histogram/issues/452 #
                if all([isinstance_(axis, (StrCategory, Boolean, IntCategory)) for axis in hists._hists[name].axes]) and len(masked) > 0:
                    broadcasted = False
                    tobroadcast = None
                    for k, v in hist_args.items():
                        if k != 'weight':
                            if isinstance_(v, AnyArray) and len(v) == len(masked):
                                broadcasted = True
                                break
                            else:
                                tobroadcast = k
                    if not broadcasted and tobroadcast is not None:
                        hist_args[tobroadcast] = np.full(len(masked), hist_args[tobroadcast])
                ############################################################
                hists._hists[name].fill(**hist_args, threads = self.threads)