import sys
from typing import Literal


class _Modules:
    ...


def fetch_imported(**modules):
    imported = _Modules()
    for k, v in modules.items():
        if v in sys.modules:
            setattr(imported, k, sys.modules[v])
    return imported


def fetch_backend(data, abbr=True):
    mod = fetch_imported(ak='awkward', pd='pandas', np='numpy')
    if isinstance(data, dict):
        backends = {*map(fetch_backend, data.values())}
        if len(backends) == 1:
            if backends.pop() == 'npy':
                return 'np' if abbr else 'numpy'
        return 'dict'
    try:
        if isinstance(data, mod.np.ndarray):
            return 'npy' if abbr else 'numpy.array'
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


def concat(data: list, library: Literal['ak', 'pd', 'np']):
    if len(data) == 0:
        return None
    if len(data) == 1:
        return data[0]
    if library == 'ak':
        import awkward as ak
        return ak.concatenate(data)
    elif library == 'pd':
        import pandas as pd
        return pd.concat(
            data,
            ignore_index=True,
            sort=False,
            copy=False)
    elif library == 'np':
        import numpy as np
        result = {}
        for k in data[0].keys():
            result[k] = np.concatenate([d[k] for d in data])
        return result
    else:
        raise ValueError(f'Unknown library {library}.')


def length(data):
    backend = fetch_backend(data, abbr=True)
    if backend in ('ak', 'pd'):
        return len(data)
    elif backend in ('np', 'dict'):
        lengths = {*map(len, data.values())}
        if len(lengths) == 1:
            return lengths.pop()
