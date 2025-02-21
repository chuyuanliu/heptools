from __future__ import annotations

import dask_awkward as dak
import numpy as np
from hist.dask import Hist

from ..aktools import RealNumber
from ..hist import H, Template
from ..hist import hist as _h
from . import awkward as dakext
from .awkward._meta import no_touch_first_array

__all__ = [
    "Collection",
    "Fill",
    "FillLike",
    "Template",
    "H",
]

FillLike = _h.LazyFill | RealNumber | bool | dak.Array


class Fill(_h._Fill[Hist]):
    class __backend__(_h._Fill.__backend__):
        check_empty_mask = False
        akarray = dak.Array
        anyarray = dak.Array
        repeat = dakext.delayed(np.repeat, meta=no_touch_first_array)


class Collection(_h._Collection[Hist, Fill]):
    class __backend__(_h._Collection.__backend__):
        fill = Fill
        hist = Hist
