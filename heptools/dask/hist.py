from __future__ import annotations

import dask_awkward as dak
import numpy as np
from hist.dask import Hist

from ..aktools import RealNumber
from ..hist import H, Template
from ..hist import hist as _h
from . import awkward as dakext
from .awkward._utils import maybe_typetracer

__all__ = [
    "Collection",
    "Fill",
    "FillLike",
    "Template",
    "H",
]

FillLike = _h.LazyFill | RealNumber | bool | dak.Array


def _np_repeat_meta(a, repeats, axis=None):
    a = maybe_typetracer(a)
    repeats = maybe_typetracer(repeats)
    return a


class Fill(_h._Fill[Hist]):
    class __backend__(_h._Fill.__backend__):
        check_empty_mask = False
        akarray = dak.Array
        anyarray = dak.Array
        repeat = dakext.delayed(np.repeat, meta=_np_repeat_meta)


class Collection(_h._Collection[Hist, Fill]):
    class __backend__(_h._Collection.__backend__):
        fill = Fill
        hist = Hist
