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

import uproot

from ..system.eos import EOS, PathLike
from . import tree
from ._backend import concat_record, len_record, record_backend, slice_record

if TYPE_CHECKING:
    import awkward as ak
    import numpy as np
    import pandas as pd

    RecordLike = ak.Array | pd.DataFrame | dict[str, np.ndarray]
    """
    ak.Array, pandas.DataFrame, dict[str, numpy.ndarray]: A mapping from string to array-like object. All array-like objects must have the same length.
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
        If not given, a new :class:`TBasket` will be created for each :meth:`extend` call. Otherwise, a buffer will be used to fix the size of :class:`TBasket`.
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
        return sum(len_record(b, self._backend) for b in self._buffer)

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
            data = concat_record(self._buffer, library=self._backend)
            self._buffer = []
        if data is not None and len(data) > 0:
            if self._backend == 'ak':
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
            Data to extend. Using :class:`dict` of :class:`numpy.ndarray` may result in slightly worse performance.

        Returns
        -------
        TreeWriter:``self``
        """
        backend = record_backend(data)
        if backend not in ('ak', 'pd', 'np'):
            raise TypeError(
                f'Unsupported data backend {type(data)}.')
        if self._basket_size is ...:
            self._backend = backend
        else:
            if self._backend is None:
                self._backend = backend
            else:
                if backend != self._backend:
                    raise TypeError(
                        f'Inconsistent data backend, expected {self._backend}, given {backend}.')
        size = len_record(data, self._backend)
        if size == 0:
            return
        elif size == None:
            raise ValueError(
                'The extended data does not have a well-defined length.')
        if self._basket_size is ...:
            self._backend = backend
            self._buffer = data
            self._flush()
        else:
            start = 0
            while start < len_record(data, self._backend):
                diff = self._basket_size - self._buffer_size
                self._buffer.append(slice_record(
                    data, start, start + diff, library=self._backend))
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
        - :func:`pandas.concat` is used for ``library='pd'``
        - :func:`numpy.concatenate` is used for ``library='np'``

        Parameters
        ----------
        sources : tuple[~heptools.root.tree.Chunk]
            One or more chunks of :class:`TTree`.
        library : {'ak', 'pd', 'np'}, optional, default='ak'
            The library used to represent arrays. ``ak`` for :class:`ak.Array`, ``pd`` for :class:`pandas.DataFrame` and ``np`` for :class:`dict` of :class:`numpy.ndarray`.
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
        if library in ('ak', 'pd', 'np'):
            return concat_record(
                [self.arrays(s, **options) for s in sources],
                library=library)
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
        *source: tree.Chunk,
        **options,
    ) -> dict[str, np.ndarray]:
        """
        Read data into :class:`dict` of :class:`numpy.ndarray`. Equivalent to :meth:`concat` with ``library='np'``.
        """
        options['library'] = 'np'
        return self.concat(*source, **options)
