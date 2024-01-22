"""
ROOT file I/O based on :func:`uproot.open` \(:func:`uproot.reading.open`\) and :func:`uproot.recreate` \(:func:`uproot.writing.writable.recreate`\).

.. warning::
    Writers will always overwrite the output file if it exists.

.. todo::
    Add lazy reading and schema support using :class:`coffea.nanoevents.NanoEventsFactory`.
"""

from typing import TYPE_CHECKING

import uproot

from ..system.eos import EOS, PathLike

if TYPE_CHECKING:
    import awkward as ak
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
        self._baset_size = basket_size
        self._reset()

    def __enter__(self):
        self._temp = self._path.local_temp(dir='.')
        self._file = uproot.recreate(self._temp)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._flush()
            self._file.close()
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
        self._buffer = None if self._baset_size is None else []
        self._backend = None

    def _flush(self):
        ...  # TODO

    def extend(self, data):
        """
        Extend the :class:`TTree` with ``data``.

        Parameters
        ----------
        data : ak.Array, pandas.DataFrame, dict[str, numpy.ndarray | ak.Array | pandas.Series]
            Data passed to :meth:`uproot.writing.writable.WritableTree.extend`.
        """
        ...  # TODO


class TreeReader:
    ...

    def array(self):
        ...  # TODO

    def dataframe(self):
        ...  # TODO
