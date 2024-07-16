from __future__ import annotations

import operator as op
import sys
from functools import partial, reduce
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import awkward
    import numpy
    import pandas

_UNKNOWN = "Unknown backend {library}."


class _Backends:
    backends = dict(ak="awkward", np="numpy", pd="pandas")
    ak: awkward
    np: numpy
    pd: pandas

    def __getattr__(self, name):
        try:
            return sys.modules[self.backends[name]]
        except KeyError:
            return self

    def check(self, __obj, __type):
        if __type is self:
            return False
        return isinstance(__obj, __type)


def record_backend(data, sequence=False):
    mod = _Backends()
    if sequence:
        backends = {*map(record_backend, data)}
        if len(backends) == 1:
            return backends.pop()
        else:
            raise ValueError(f"Inconsistent backends {backends}.")
    if isinstance(data, dict):
        backends = {*map(record_backend, data.values())}
        if len(backends) == 0:
            return "np"
        if len(backends) == 1:
            backends = backends.pop()
            if backends == "np.array":
                return "np"
            else:
                return f"dict.{backends}"
        return "dict"
    if mod.check(data, mod.ak.Array):
        return "ak"
    if mod.check(data, mod.np.ndarray):
        return "np.array"
    if mod.check(data, mod.pd.DataFrame):
        return "pd"
    return f"<{data.__module__}.{data.__class__.__name__}>"


def concat_record(data: list, library: Literal["ak", "pd", "np"] = ...):
    if library is ...:
        library = record_backend(data, sequence=True)
    if len(data) == 0:
        return None
    data = [data[0], *filter(partial(len_record, library=library), data[1:])]
    if len(data) == 1:
        return data[0]
    if library == "ak":
        import awkward as ak

        return ak.concatenate(data)
    elif library == "pd":
        import pandas as pd

        return pd.concat(data, ignore_index=True, sort=False, copy=False, axis=0)
    elif library == "np":
        import numpy as np

        result = {}
        for k in data[0].keys():
            result[k] = np.concatenate([d[k] for d in data])
        return result
    else:
        raise TypeError(_UNKNOWN.format(library=library))


def merge_record(data: list, library: Literal["ak", "pd", "np"] = ...):
    if library is ...:
        library = record_backend(data, sequence=True)
    if len(data) == 0:
        return None
    if len(data) == 1:
        return data[0]
    if library == "ak":
        import awkward as ak

        return ak.zip(
            reduce(op.or_, (dict(zip(ak.fields(arr), ak.unzip(arr))) for arr in data)),
            depth_limit=1,
        )
    elif library == "pd":
        import pandas as pd

        return pd.concat(data, ignore_index=False, sort=False, copy=False, axis=1)
    elif library == "np" or library.startswith("dict"):
        return reduce(op.or_, data)
    else:
        raise TypeError(_UNKNOWN.format(library=library))


def slice_record(data, start: int, stop: int, library: Literal["ak", "pd", "np"] = ...):
    if library is ...:
        library = record_backend(data)
    if library in ("ak", "pd"):
        return data[start:stop]
    elif library == "np":
        return {k: v[start:stop] for k, v in data.items()}
    elif library.startswith("dict"):
        if library == "dict":
            return {k: slice_record(v, start, stop) for k, v in data.items()}
        else:
            content = library.removeprefix("dict.")
            return {
                k: slice_record(v, start, stop, library=content)
                for k, v in data.items()
            }
    else:
        raise TypeError(_UNKNOWN.format(library=library))


def len_record(data, library: Literal["ak", "pd", "np"] = ...):
    if library is ...:
        library = record_backend(data)
    if library in ("ak", "pd"):
        return len(data)
    elif library == "np" or library.startswith("dict"):
        if len(data) == 0:
            return 0
        return len(next(iter(data.values())))
    else:
        raise TypeError(_UNKNOWN.format(library=library))


def rename_record(data, mapping, library: Literal["ak", "pd", "np"] = ...):
    if library is ...:
        library = record_backend(data)
    if library == "ak":
        import awkward as ak

        return ak.zip(
            dict(zip(map(mapping, ak.fields(data)), ak.unzip(data))), depth_limit=1
        )
    elif library == "pd":
        return data.rename(columns=mapping, copy=False)
    elif library == "np" or library.startswith("dict"):
        return dict(zip(map(mapping, data.keys()), data.values()))
    else:
        raise TypeError(_UNKNOWN.format(library=library))


def sizeof_record(data, library: Literal["ak", "pd", "np"] = ...):
    if library is ...:
        library = record_backend(data)
    if library in ("ak", "np.array"):
        return data.nbytes
    elif library == "pd":
        return data.memory_usage(index=True, deep=True).sum()
    elif library == "np" or library.startswith("dict"):
        if library == "dict":
            lib = ...
        elif library == "np":
            lib = "np.array"
        else:
            lib = library.removeprefix("dict.")
        return sum(sizeof_record(v, library=lib) for v in data.values())
    else:
        raise TypeError(_UNKNOWN.format(library=library))
