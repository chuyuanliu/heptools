# TODO test
from __future__ import annotations

from typing import Callable

import awkward as ak
import dask_awkward as dak
import numpy as np
from hist.axis import StrCategory
from hist.dask import Hist

from .. import hist as _h
from ..aktools import FieldLike, RealNumber, and_fields, get_field
from ..dask.awkward import map_partitions
from ..typetools import check_type

__all__ = ['Collection', 'Fill', 'FillLike']

FillLike = _h.hist.LazyFill | RealNumber | bool


_np_repeat = map_partitions(np.repeat)


class Fill(_h.Fill):
    _dast = True

    def fill(self, events: ak.Array, hists: Collection = ..., **fill_args: FillLike):
        if hists is ...:
            if Collection.current is None:
                raise _h.FillError('no histogram collection is specified')
            hists = Collection.current
        fill_args = self._kwargs | fill_args
        mask_categories = []
        for category in hists._categories:
            if category not in fill_args:
                if isinstance(hists._axes[category], StrCategory):
                    mask_categories.append(category)
                else:
                    fill_args[category] = _h.hist._default_field(category)
        for category_args in hists._generate_category_combinations(mask_categories):
            mask = and_fields(events, *category_args.items())
            masked = events if mask is None else events[mask]
            for k, v in fill_args.items():
                if (isinstance(v, str) and k in hists._categories) or isinstance(v, (bool, RealNumber)):
                    category_args[k] = v
                elif check_type(v, FieldLike):
                    category_args[k] = get_field(masked, v)
                elif check_type(v, Callable):
                    category_args[k] = v(masked)
                else:
                    raise _h.FillError(f'cannot fill "{k}" with "{v}"')
            for name in self._fills:
                fills = {}
                to_repeat = []
                count = None
                for k in self._fills[name]:
                    v = category_args[f'{name}:{k}' if f'{name}:{k}' in category_args else k]
                    if isinstance(v, dak.Array):
                        depth = v._meta.layout.minmax_depth[0]
                        if depth == 1:
                            to_repeat.append(k)
                        elif depth > 1:
                            if count is None:
                                count = ak.num(v)
                            v = ak.flatten(v)
                    fills[k] = v
                if count is not None:
                    for k in to_repeat:
                        fills[k] = _np_repeat(fills[k], count)
                hists._hists[name].fill(**fills)


class Collection(_h.Collection):
    _backend_fill = Fill
    _backend_hist = Hist
