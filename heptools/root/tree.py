# TODO
from typing import Any, Iterable
from uuid import UUID

from ..system.eos import EOS, PathLike
from ..typetools import check_type
from ._utils import fetch_backend


class Tree:
    """
    :class:`ROOT.TTree` like data structure.

    Parameters
    ----------
    source : PathLike, tuple[PathLike, ~uuid.UUID], ak.Array, pandas.DataFrame, dict[str, numpy.ndarray | ak.Array | pandas.Series]
        Source of data.

        - path to ROOT file with optional UUID
        - data supported by :class:`uproot.writing.writable.WritableTree`

    num_entries : int, optional, default=None
        Number of entries. If ``None``, infer from ``source``.
    branches : ~typing.Iterable[str], optional, default=None
        Name of branches. If ``None``, infer from ``source``.
    """

    def __init__(
        self,
        source: PathLike | tuple[PathLike, UUID] | Any,
        num_entries: int = None,
        branches: Iterable[str] = None,
    ):
        self._num_entries = num_entries
        self._branches = branches

        self._path: EOS = None
        self._uuid: UUID = None
        self._data = None
        self._backend: str = None

        if check_type(source, PathLike):
            self._path = EOS(source)
        elif check_type(source, tuple[PathLike, UUID]):
            self._path = EOS(source[0])
            self._uuid = source[1]
        else:
            self._data = source
            self._backend = fetch_backend(self._data)
