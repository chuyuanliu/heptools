"""
ROOT file I/O based on :func:`uproot.reading.open` and :func:`uproot.writing.writable.recreate`.

.. note::
    Readers will use the following default options for :func:`uproot.open`:
    
    .. code-block:: python

        object_cache=None
        array_cache=None
        timeout=3 * 60

.. warning::
    Writers will always overwrite the output file if it exists.

.. todo::
    Add lazy reading and schema support using :class:`coffea.nanoevents.NanoEventsFactory`.

.. todo::
    Consider migrating to `fsspec-xrootd <https://coffeateam.github.io/fsspec-xrootd/>`_
"""
from __future__ import annotations

import gc
from typing import TYPE_CHECKING, Callable, Literal

import awkward as ak
import uproot

from ..system.eos import EOS, PathLike
from . import tree
from ._backend import fetch_backend

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

    RecordLike = ak.Array | pd.DataFrame | dict[str,
                                                np.ndarray | pd.Series | ak.Array]
    """
    ak.Array, pandas.DataFrame, dict[str, numpy.ndarray | pandas.Series | ak.Array]: A mapping from string to array-like object. All array-like objects must have the same length.
    """


class _Reader:
    _default_options = {
        'object_cache': None,
        'array_cache': None,
        'timeout': 3 * 60,
    }

    def __init__(self, **options):
        self._options = self._default_options | options


class TreeWriter:
    """
    :func:`uproot.recreate` with remote file support and :class:`TBasket` size control.

    Parameters
    ----------
    name : str, optional, default='Events'
        Name of tree.
    parents : bool, optional, default=True
        Create parent directories if not exist.
    basket_size : int, optional
        If not given, a new :class:`TBasket` will be created for each :meth:`extend` call. Otherwise, a buffer will be used to fix the size of :class:`TBasket`. Only available when the data passed to :meth:`extend` is always :class:`ak.Array` or :class:`pandas.DataFrame`.
    **options: dict, optional
        Additional options passed to :func:`uproot.recreate`.
    Attributes
    ----------
    tree : ~heptools.root.tree.Chunk
        Created :class:`TTree`.
    """

    def __init__(
            self,
            name: str = 'Events',
            parents: bool = True,
            basket_size: int = ...,
            **options):
        self._name = name
        self._parents = parents
        self._basket_size = basket_size
        self._options = options

        self.tree: tree.Chunk = None
        self._reset()

    def __call__(self, path: PathLike):
        """
        Set output path.

        Parameters
        ----------
        path : PathLike
            Path to output ROOT file.

        Returns
        -------
        TreeWriter:``self``
        """
        self._path = EOS(path)
        return self

    def __enter__(self):
        """
        Open a temporary local ROOT file for writing.

        Returns
        -------
        TreeWriter:``self``
        """
        self.tree = None
        self._temp = self._path.local_temp(dir='.')
        self._file = uproot.recreate(self._temp, **self._options)
        return self

    def __exit__(self, *exc):
        """
        If no exception is raised, move the temporary file to the output path and store :class:`~heptools.root.tree.Chunk` information to :data:`tree`.
        """
        if not any(exc):
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
        self._path = None
        self._temp = None
        self._file = None
        self._buffer = None if self._basket_size is ... else []
        self._backend = None

    def _flush(self):
        if self._basket_size is ...:
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
            if self._backend == 'awkward':
                if data.layout.minmax_depth[1] > 1:
                    data = {k: data[k] for k in data.fields}
            if self._name not in self._file:
                self._file[self._name] = data
            else:
                self._file[self._name].extend(data)
        data = None
        gc.collect()

    def extend(
            self,
            data: RecordLike):
        """
        Extend the :class:`TTree` with ``data`` using :meth:`uproot.writing.writable.WritableTree.extend`.

        Parameters
        ----------
        data : RecordLike
            Data to extend.

        Returns
        -------
        TreeWriter:``self``
        """
        backend = fetch_backend(data)
        if self._basket_size is ...:
            self._backend = backend
            self._buffer = data
            self._flush()
        else:
            if self._backend is None:
                self._backend = backend
                if self._backend not in {'awkward', 'pandas'}:
                    raise TypeError(
                        f'Fixed basket size is only available for ak.Array or pd.DataFrame, given {self._backend}.')
            else:
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


class TreeReader(_Reader):
    """
    Read data from :class:`~heptools.root.tree.Chunk`.

    Parameters
    ----------
    filter_branch : ~typing.Callable[[set[str]], set[str]], optional
        Function to select branches. If not given, all branches will be read.
    **options : dict, optional
        Additional options passed to :func:`uproot.open`.
    """

    def __init__(
        self,
        filter_branch: Callable[[set[str]], set[str]] = None,
        **options,
    ):
        super().__init__(**options)
        self._filter = filter_branch

    def arrays(
        self,
        source: tree.Chunk,
        **options,
    ) -> RecordLike:
        """
        Read data into arrays.

        Parameters
        ----------
        source : ~heptools.root.tree.Chunk
            Chunk of :class:`TTree`.
        **options : dict, optional
            Additional options passed to :meth:`uproot.behaviors.TBranch.HasBranches.arrays`.

        Returns
        -------
        RecordLike
            Data read from :class:`TTree`.
        """
        branches = source.branches
        if self._filter is not None:
            branches = self._filter(branches)
        with uproot.open(source.path, **self._options) as file:
            return file[source.name].arrays(
                expressions=branches,
                entry_start=source.entry_start,
                entry_stop=source.entry_stop,
                **options)

    def concat(
        self,
        *sources: tree.Chunk,
        library: Literal['ak', 'pd', 'np'] = 'ak',
        **options,
    ) -> RecordLike:
        """
        Read multiple ``sources`` into one array.

        .. todo::
            Add :mod:`multiprocessing` support.

        - :func:`ak.concatenate` is used for ``library='ak'``
        - :func:`pandas.concat` is used for ``library='pd'``.

        Parameters
        ----------
        sources : tuple[~heptools.root.tree.Chunk]
            One or more chunks of :class:`TTree`.
        **options : dict, optional
            Additional options passed to :meth:`arrays`.

        Returns
        -------
        RecordLike
            Concatenated data.
        """
        options['library'] = library
        if len(sources) == 1:
            return self.arrays(sources[0], **options)
        if library == 'ak':
            return ak.concatenate([self.arrays(s, **options) for s in sources])
        elif library == 'pd':
            import pandas as pd
            return pd.concat(
                [self.arrays(s, **options) for s in sources],
                ignore_index=True,
                sort=False,
                copy=False)
        elif library == 'np':
            raise NotImplementedError
        else:
            raise ValueError(f'Unknown library {library}.')

    def Array(
        self,
        *source: tree.Chunk,
        **options,
    ) -> ak.Array:
        """
        Read data into :class:`ak.Array`. Equivalent to :meth:`concat` with ``library='ak'``.
        """
        options['library'] = 'ak'
        return self.concat(*source, **options)

    def DataFrame(
        self,
        *source: tree.Chunk,
        **options,
    ) -> pd.DataFrame:
        """
        Read data into :class:`pandas.DataFrame`. Equivalent to :meth:`concat` with ``library='pd'``.
        """
        options['library'] = 'pd'
        return self.concat(*source, **options)

    def NDArray(
        self,
        source: tree.Chunk,
        **options,
    ) -> dict[str, np.ndarray]:
        """
        Read data into :class:`dict` of :class:`numpy.ndarray`. Equivalent to :meth:`arrays` with ``library='np'``.
        """
        options['library'] = 'np'
        return self.arrays(source, **options)
