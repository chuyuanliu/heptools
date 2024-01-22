import sys


class _Modules:
    ...


def fetch_imported(**modules):
    imported = _Modules()
    for k, v in modules.items():
        if v in sys.modules:
            setattr(imported, k, sys.modules[v])
    return imported


def fetch_backend(data):
    mod = fetch_imported(ak='awkward', pd='pandas', np='numpy')
    if isinstance(data, dict):
        return 'dict'
    try:
        if isinstance(data, mod.np.ndarray):
            return 'numpy'
    except:
        pass
    try:
        if isinstance(data, mod.ak.Array):
            return 'awkward'
    except:
        pass
    try:
        if isinstance(data, mod.pd.DataFrame):
            return 'pandas'
    except:
        pass
