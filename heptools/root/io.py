"""
ROOT file I/O based on :func:`uproot.open` \(:func:`uproot.reading.open`\) and :func:`uproot.recreate` \(:func:`uproot.writing.writable.recreate`\).

.. warning::
    Writers will always overwrite the output file if it exists.

.. todo::
    Add lazy reading and schema support using :class:`coffea.nanoevents.NanoEventsFactory`.
"""
from __future__ import annotations

import gc
from typing import TYPE_CHECKING

import awkward as ak
import uproot

from ..system.eos import EOS, PathLike
from . import tree
from ._utils import fetch_backend

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd


class TreeWriter:
    """
    :func:`uproot.recreate` with remote file support and :class:`TBasket` size control.

    Parameters
    ----------
    path : PathLike
        Path to output file.
    name : str, optional, default='Events'
        Name of tree.
    parents : bool, optional, default=True
        Create parent directories if not exist.
    basket_size : int, optional, default=None
        If ``None``, a new :class:`TBasket` will be created for each :meth:`extend` call. Otherwise, a buffer will be used to fix the size of :class:`TBasket`. Only available when the data passed to :meth:`extend` is always :class:`ak.Array` or :class:`pandas.DataFrame`.

    Attributes
    ----------
    tree : ~heptools.root.tree.Chunk
        Created :class:`TTree`.
    """

    def __init__(
            self,
            path: PathLike,
            name: str = 'Events',
            parents: bool = True,
            basket_size: int = None,):
        self._path = EOS(path)
        self._name = name
        self._parents = parents
        self._basket_size = basket_size
        self.tree: tree.Chunk = None
        self._reset()

    def __enter__(self):
        self.tree = None
        self._temp = self._path.local_temp(dir='.')
        self._file = uproot.recreate(self._temp)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._flush()
            self._file.close()
            self.tree = tree.Chunk(
                source=self._temp,
                name=self._name,
                fetch=True)
            self.tree.path = self._path
            self._temp.move_to(
                self._path, parents=self._parents, overwrite=True)
        else:
            self._file.close()
            self._temp.rm()
        self._reset()

    @property
    def _buffer_size(self):
        return sum(len(b) for b in self._buffer)

    def _reset(self):
        self._temp = None
        self._file = None
        self._buffer = None if self._basket_size is None else []
        self._backend = None

    def _flush(self):
        if self._basket_size is None:
            data = self._buffer
            self._buffer = None
        else:
            if self._buffer_size == 0:
                data = None
            else:
                if self._backend == 'awkward':
                    import awkward as ak
                    data = ak.concatenate(self._buffer)
                elif self._backend == 'pandas':
                    import pandas as pd
                    data = pd.concat(self._buffer,
                                     ignore_index=True,
                                     sort=False,
                                     copy=False)
            self._buffer = []
        if data is not None and len(data) > 0:
            if self._name not in self._file:
                self._file[self._name] = data
            else:
                self._file[self._name].extend(data)
        data = None
        gc.collect()

    def extend(
            self,
            data: ak.Array | pd.DataFrame | dict[str, ak.Array | pd.Series | np.ndarray]):
        """
        Extend the :class:`TTree` with ``data``.

        Parameters
        ----------
        data : ak.Array, pandas.DataFrame, dict[str, numpy.ndarray | ak.Array | pandas.Series]
            Data passed to :meth:`uproot.writing.writable.WritableTree.extend`.
        """
        if self._basket_size is None:
            self._buffer = data
            self._flush()
        else:
            if self._backend is None:
                self._backend = fetch_backend(data)
                if self._backend not in {'awkward', 'pandas'}:
                    raise TypeError(
                        f'Fixed basket size is only available for ak.Array or pd.DataFrame, given {self._backend}.')
            else:
                backend = fetch_backend(data)
                if backend != self._backend:
                    raise TypeError(
                        f'Inconsistent data backend, expected {self._backend}, given {backend}.')
            start = 0
            while start < len(data):
                diff = self._basket_size - self._buffer_size
                self._buffer.append(data[start: start + diff])
                start += diff
                if self._buffer_size >= self._basket_size:
                    self._flush()
        return self


class TreeReader:  # TODO
    ...

    def array(self):
        ...  # TODO

    def dataframe(self):
        ...  # TODO
