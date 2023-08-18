from __future__ import annotations

import gc
import operator
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import reduce
from typing import Callable

import awkward as ak
import numpy as np
import uproot

from ..utils import match_any

__all__ = ['Skim', 'PicoAOD',
           'Buffer', 'BasketSizeOptimizedBuffer', 'NoBuffer']

class Buffer(ABC):
    path: str
    tree: str
    jagged: list[str]

    def __init__(self):
        self._file: uproot.WritableDirectory = None
        self._buffer: ak.Array = None

    def __call__(self, path: str, tree: str, jagged: list[str] = None):
        new = self.copy()
        new.path = path
        new.tree = tree
        new.jagged = jagged
        return new

    def copy(self):
        return self.__class__()

    def flush(self):
        if self._file is not None and self._buffer is not None:
            self.before_flush()
            buffer = defaultdict(dict)
            for k in self._buffer.fields:
                is_jagged = False
                for jagged in self.jagged:
                    if k == f'n{jagged}':
                        is_jagged = True
                    elif k.startswith(f'{jagged}_'):
                        is_jagged = True
                        buffer[jagged][k.removeprefix(f'{jagged}_')] = self._buffer[k]
                    if is_jagged:
                        break
                if not is_jagged:
                    buffer[k] = self._buffer[k]
            for jagged in self.jagged:
                if jagged in buffer:
                    buffer[jagged] = ak.zip(buffer[jagged])
            if self.tree in self._file:
                self._file[self.tree].extend(buffer)
            else:
                self._file[self.tree] = buffer
            self._buffer = None

    @abstractmethod
    def add_block(self, block: ak.Array):
        ...

    @abstractmethod
    def before_flush(self):
        ...

    def __iadd__(self, block: ak.Array):
        self.add_block(block)
        gc.collect()
        return self

    def __enter__(self):
        if self.jagged is None:
            self.jagged = []
        self._file = uproot.recreate(self.path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()
        self._file.close()

class BasketSizeOptimizedBuffer(Buffer):
    def __init__(self, buffer_size: int = 100_000):
        self.size = buffer_size
        super().__init__()

    def copy(self):
        return self.__class__(self.size)

    @property
    def occupied(self):
        return 0 if self._buffer is None else sum(len(b) for b in self._buffer)

    def add_block(self, block: ak.Array):
        start = 0
        while start < len(block):
            if self._buffer is None:
                self._buffer = []
            diff = self.size - self.occupied
            self._buffer.append(block[start: start + diff])
            start += diff
            if self.occupied >= self.size:
                self.flush()
        return self

    def before_flush(self):
        if self._buffer is not None:
            self._buffer = ak.concatenate(self._buffer)

class NoBuffer(Buffer):
    def add_block(self, block: ak.Array):
        self._buffer = block
        self.flush()

    def before_flush(self):
        ...

class Skim:
    def __init__(self, jagged: list[str], excluded: list[str | re.Pattern] = None, metadata = None, # TODO
                 unique_index: str = None,
                 iterate_step: int | str = '500 MB', buffer: Buffer = NoBuffer()):
        self.jagged = jagged
        self.excluded = excluded if excluded is not None else []
        self.metadata = metadata # TODO
        self.unique_index = unique_index
        self.iterate_step = iterate_step
        self.buffer = buffer
        self.timeout  = 7 * 24 * 60

    def copy(self):
        return self.__class__(self.jagged, self.excluded, self.metadata,
                              self.unique_index, self.iterate_step, self.buffer.copy())

    def _get_branches(self, file):
        with uproot.open(file, timeout = self.timeout) as f:
            branches = set(f['Events'].keys())
        return {b for b in branches if not match_any(b, self.excluded, lambda x, y: re.match(y, x) is not None)}

    def create(self, output: str, files: list[str], selection: Callable[[ak.Array], ak.Array] = None, allow_multiprocessing: bool = False):
        nevents = 0
        with self.buffer(output, 'Events', self.jagged) as output:
            if allow_multiprocessing:
                import multiprocessing as mp
                with mp.Pool(len(files)) as pool:
                    branches = pool.map(self._get_branches, files)
            else:
                branches = [self._get_branches(file) for file in files]
            branches = reduce(operator.and_, branches)
            for chunk in uproot.iterate([f'{f}:Events' for f in files], expressions = branches, step_size = self.iterate_step, timeout = self.timeout):
                if selection is not None:
                    chunk = selection(chunk)
                if self.unique_index is not None:
                    chunk[self.unique_index] = np.arange(nevents, nevents + len(chunk), dtype = np.uint64)
                nevents += len(chunk)
                output += chunk
        return nevents

PicoAOD = Skim(
    jagged = [
        'Jet', 'FatJet', 'SubJet', 'CorrT1METJet', 'SoftActivityJet',
        'Photon', 'Electron', 'Muon', 'Tau', 'FsrPhoton', 'LowPtElectron', 'boostedTau',
        'Proton_singleRP', 'Proton_multiRP',
        'SV', 'OtherPV', 'PPSLocalTrack', 'IsoTrack',
        'TrigObj'
    ], unique_index = 'eventPico')