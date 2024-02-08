import operator as op
import sys
from functools import partial, reduce
from typing import Literal


class _Modules:
    ...


_unknown_msg = 'Unknown backend "{library}".'


def fetch_imported(**modules):
    imported = _Modules()
    for k, v in modules.items():
        if v in sys.modules:
            setattr(imported, k, sys.modules[v])
    return imported


def record_backend(data, abbr=True, sequence=False):
    mod = fetch_imported(ak='awkward', pd='pandas', np='numpy')
    if sequence:
        backends = {*map(partial(record_backend, abbr=abbr), data)}
        if len(backends) == 1:
            return backends.pop()
        else:
            raise ValueError(f'Inconsistent backends {backends}.')
    if isinstance(data, dict):
        backends = {*map(partial(record_backend, abbr=abbr), data.values())}
        if len(backends) == 0:
            return 'np' if abbr else 'numpy'
        if len(backends) == 1:
            backends = backends.pop()
            if backends in ('np.array', 'numpy.array'):
                return 'np' if abbr else 'numpy'
            else:
                return f'dict.{backends}'
        return 'dict'
    try:
        if isinstance(data, mod.np.ndarray):
            return 'np.array' if abbr else 'numpy.array'
    except:
        pass
    try:
        if isinstance(data, mod.ak.Array):
            return 'ak' if abbr else 'awkward'
    except:
        pass
    try:
        if isinstance(data, mod.pd.DataFrame):
            return 'pd' if abbr else 'pandas'
    except:
        pass
    return type(data).__name__


def concat_record(data: list, library: Literal['ak', 'pd', 'np'] = ...):
    if library is ...:
        library = record_backend(data, abbr=True, sequence=True)
    if len(data) == 0:
        return None
    data = [data[0], *filter(partial(len_record, library=library), data[1:])]
    if len(data) == 1:
        return data[0]
    if library == 'ak':
        import awkward as ak
        return ak.concatenate(data)
    elif library == 'pd':
        import pandas as pd
        return pd.concat(data, ignore_index=True, sort=False, copy=False, axis=0)
    elif library == 'np':
        import numpy as np
        result = {}
        for k in data[0].keys():
            result[k] = np.concatenate([d[k] for d in data])
        return result
    else:
        raise ValueError(_unknown_msg.format(library=library))


def merge_record(data: list, library: Literal['ak', 'pd', 'np'] = ...):
    if library is ...:
        library = record_backend(data, abbr=True, sequence=True)
    if len(data) == 0:
        return None
    if len(data) == 1:
        return data[0]
    if library == 'ak':
        import awkward as ak
        return ak.zip(reduce(op.or_, (dict(zip(ak.fields(arr), ak.unzip(arr))) for arr in data)), depth_limit=1)
    elif library == 'pd':
        import pandas as pd
        return pd.concat(data, ignore_index=False, sort=False, copy=False, axis=1)
    elif library == 'np' or library.startswith('dict'):
        return reduce(op.or_, data)
    else:
        raise ValueError(_unknown_msg.format(library=library))


def slice_record(data, start: int, stop: int, library: Literal['ak', 'pd', 'np'] = ...):
    if library is ...:
        library = record_backend(data, abbr=True)
    if library in ('ak', 'pd'):
        return data[start:stop]
    elif library == 'np':
        return {k: v[start:stop] for k, v in data.items()}
    elif library.startswith('dict'):
        if library == 'dict':
            return {k: slice_record(v, start, stop) for k, v in data.items()}
        else:
            content = library.removeprefix('dict.')
            return {k: slice_record(v, start, stop, library=content) for k, v in data.items()}
    else:
        raise ValueError(_unknown_msg.format(library=library))


def len_record(data, library: Literal['ak', 'pd', 'np'] = ...):
    if library is ...:
        library = record_backend(data, abbr=True)
    if library in ('ak', 'pd'):
        return len(data)
    elif library == 'np' or library.startswith('dict'):
        if len(data) == 0:
            return 0
        return len(next(iter(data.values())))
    else:
        raise ValueError(_unknown_msg.format(library=library))


def rename_record(data, mapping, library: Literal['ak', 'pd', 'np'] = ...):
    if library is ...:
        library = record_backend(data, abbr=True)
    if library == 'ak':
        import awkward as ak
        return ak.zip(dict(zip(map(mapping, ak.fields(data)), ak.unzip(data))), depth_limit=1)
    elif library == 'pd':
        return data.rename(columns=mapping, copy=False)
    elif library == 'np' or library.startswith('dict'):
        return dict(zip(map(mapping, data.keys()), data.values()))
    else:
        raise ValueError(_unknown_msg.format(library=library))
