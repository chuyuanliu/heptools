import importlib
import pickle
from typing import Literal

Compression = Literal['gzip', 'bz2', 'lzma']

def open_zip(algo: Compression, file: str, mode: str, **kwargs):
    mod = importlib.import_module(algo)
    default = {}
    if algo in ['gzip', 'bz2']:
        default['compresslevel'] = 4
    return mod.open(file, mode, **(default | kwargs))

def save(fname: str, obj, algo: Compression = 'gzip', **kwargs):
    pickle.dump(obj, open_zip(algo, fname, 'wb', **kwargs))

def load(fname: str, algo: Compression = 'gzip', **kwargs):
    return pickle.load(open_zip(algo, fname, 'rb', **kwargs))