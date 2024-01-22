# TODO
from functools import partial
from typing import Iterable
from uuid import UUID

import uproot

from ..system.eos import EOS, PathLike
from ..typetools import check_type
from . import io as root_io


class _ChunkMeta(type):
    def _get(self, attr):
        if getattr(self, attr) is None:
            self._fetch()
        return getattr(self, attr)

    def __new__(cls, name, bases, dic):
        for attr in ('branches', 'num_entries', 'entry_stop', 'uuid'):
            dic[attr] = property(partial(cls._get, attr=f'_{attr}'))
        return super().__new__(cls, name, bases, dic)


class Chunk(metaclass=_ChunkMeta):  # TODO
    """
    A chunk of :class:`TTree` stored in a ROOT file.

    Parameters
    ----------
    source : PathLike, tuple[PathLike, ~uuid.UUID]
        Path to ROOT file with optional UUID
    name : str, optional, default='Events'
        Name of :class:`TTree`.
    branches : ~typing.Iterable[str], optional, default=None
        Name of branches. If ``None``, read from ``source``.
    num_entries : int, optional, default=None
        Number of entries. If ``None``, read from ``source``.
    entry_start : int, optional, default=None
        Start entry. If ``None``, set to 0.
    entry_stop : int, optional, default=None
        Stop entry. If ``None``,  set to ``num_entries``.
    fetch : bool, optional, default=False
        Fetch missing information from ``source`` immediately after initialization.
    """
    path: EOS
    '''~heptools.system.eos.EOS:Path to ROOT file.'''
    uuid: UUID
    '''~uuid.UUID:UUID of ROOT file.'''
    name: str
    '''str:Name of :class:`TTree`.'''
    branches: set[str]
    '''~typing.Set[str]:Name of branches.'''
    num_entries: int
    '''int:Number of entries.'''
    entry_start: int
    '''int:Start entry.'''
    entry_stop: int
    '''int:Stop entry.'''

    def __init__(
        self,
        source: PathLike | tuple[PathLike, UUID],
        name: str = 'Events',
        branches: Iterable[str] = None,
        num_entries: int = None,
        entry_start: int = None,
        entry_stop: int = None,
        fetch: bool = False,
    ):
        if branches is not None:
            branches = {*branches}

        self.name = name
        self.path = None
        self.entry_start = entry_start or 0

        self._uuid = None
        self._branches = branches
        self._num_entries = num_entries
        self._entry_stop = entry_stop or num_entries

        if check_type(source, PathLike):
            self.path = EOS(source)
        elif check_type(source, tuple[PathLike, UUID]):
            self.path = EOS(source[0])
            self._uuid = source[1]

        if fetch:
            self._fetch()

    def _fetch(self):
        if any(v is None for v in (self._branches, self._num_entries, self._uuid)):
            with uproot.open(self.path) as file:
                tree = file[self.name]
                if self._branches is None:
                    self._branches = {*tree.keys()}
                if self._num_entries is None:
                    self._num_entries = tree.num_entries
                    self._entry_stop = self._entry_stop or self._num_entries
                if self._uuid is None:
                    self._uuid = file.file.uuid

    def __len__(self):
        return self.entry_stop - self.entry_start

    def array(self):
        ...  # TODO

    @classmethod
    def from_path(cls, *paths: str, threads: int = 1):
        ...

    @classmethod
    def iterate(cls):
        ...  # TODO yield tuple of Tree for given size


class Friend:  # TODO
    """
    A tool to create and manage addtional :class:`TBranch` stored in separate ROOT files. (also known as friend :class:`TTree`)
    """

    def __init__(self):
        ...  # TODO

    def add(self):
        ...  # TODO

    def get(self):
        ...  # TODO

    def dump(self):
        ...  # TODO

    def merge(self):
        ...  # TODO

    def move(self):
        ...  # TODO


class Chain:  # TODO
    """
    A :class:`TChain` like object to manage multiple :class:`Chunk` and :class:`Friend`.
    """

    def append(self):
        ...  # TODO

    def extend(self):
        ...  # TODO

    def add_friend(self):
        ...  # TODO

    def iterate(self):
        ...  # TODO
