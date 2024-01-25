# TODO
from __future__ import annotations

from functools import partial
from threading import Thread
from typing import Iterable
from uuid import UUID

import awkward as ak
import uproot

from ..system.eos import EOS, PathLike
from ..typetools import check_type
from . import io as root_io


class _ChunkMeta(type):
    def _get(self, attr):
        if getattr(self, attr) is ...:
            self._fetch()
        return getattr(self, attr)

    def __new__(cls, name, bases, dic):
        for attr in ('branches', 'num_entries', 'entry_stop', 'uuid'):
            dic[attr] = property(partial(cls._get, attr=f'_{attr}'))
        return super().__new__(cls, name, bases, dic)


class Chunk(metaclass=_ChunkMeta):
    """
    A chunk of :class:`TTree` stored in a ROOT file.

    Parameters
    ----------
    source : PathLike, tuple[PathLike, ~uuid.UUID]
        Path to ROOT file with optional UUID
    name : str, optional, default='Events'
        Name of :class:`TTree`.
    branches : ~typing.Iterable[str], optional
        Name of branches. If not given, read from ``source``.
    num_entries : int, optional
        Number of entries. If not given, read from ``source``.
    entry_start : int, optional
        Start entry. If not given, set to ``0``.
    entry_stop : int, optional
        Stop entry. If not given, set to ``num_entries``.
    fetch : bool, optional, default=False
        Fetch missing metadata from ``source`` immediately after initialization.

    Notes
    -----
    The following special methods are implemented:

    - :meth:`__hash__`
    - :meth:`__eq__`
    - :meth:`__len__`
    - :meth:`__repr__`
    - :meth:`__json__`
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

    @property
    def offset(self):
        '''int:Equal to ``entry_start``.'''
        return self.entry_start

    def __init__(
        self,
        source: PathLike | tuple[PathLike, UUID],
        name: str = 'Events',
        branches: Iterable[str] = ...,
        num_entries: int = ...,
        entry_start: int = ...,
        entry_stop: int = ...,
        fetch: bool = False,
    ):
        if branches is not ...:
            branches = {*branches}

        self.name = name
        self.entry_start = 0 if entry_start is ... else entry_start

        self._uuid = ...
        self._branches = branches
        self._num_entries = num_entries
        self._entry_stop = num_entries if entry_stop is ... else entry_stop

        if check_type(source, PathLike):
            self.path = EOS(source)
        elif check_type(source, tuple[PathLike, UUID]):
            self.path = EOS(source[0])
            self._uuid = source[1]

        if fetch:
            self._fetch()

    def _fetch(self, force: bool = False):
        if force or any(v is ... for v in (self._branches, self._num_entries, self._uuid)):
            with uproot.open(self.path) as file:
                tree = file[self.name]

                if self._branches is ...:
                    self._branches = {*tree.keys()}

                num_entries = tree.num_entries
                if self._num_entries is ...:
                    self._num_entries = num_entries
                    if self._entry_stop is ...:
                        self._entry_stop = num_entries
                elif self._num_entries != num_entries:
                    raise ValueError(
                        f'Inconsistent number of entries: [self]{self._num_entries} != [file]{num_entries}')

                uuid = file.file.uuid
                if self._uuid is ...:
                    self._uuid = uuid
                elif self._uuid != uuid:
                    raise ValueError(
                        f'Inconsistent UUID: [self]{self._uuid} != [file]{uuid}')

    def __hash__(self):
        return hash((self.uuid, self.name, self.num_entries))

    def __eq__(self, other):
        if isinstance(other, Chunk):
            return (self.uuid, self.name, self.num_entries) == (other.uuid, other.name, other.num_entries)
        return NotImplemented

    def __len__(self):
        return self.entry_stop - self.entry_start

    def __repr__(self):
        return f'TTree:{self.path}({self.uuid}):{self.name}[{self.entry_start},{self.entry_stop}) \u2286 [0,{self.num_entries})'

    def __json__(self):
        return {
            'path': str(self.path),
            'uuid': str(self.uuid),
            'name': self.name,
            'branches': list(self.branches),
            'num_entries': self.num_entries,
            'entry_start': self.entry_start,
            'entry_stop': self.entry_stop,
        }

    def copy(self):
        """
        Returns
        -------
        Chunk
            A deep copy of ``self``.
        """
        path = self.path if self._uuid is ... else (self.path, self._uuid)
        return Chunk(
            source=path,
            name=self.name,
            num_entries=self._num_entries,
            branches=self._branches,
            entry_start=self.entry_start,
            entry_stop=self._entry_stop)

    def slice(self, start: int, stop: int):
        """
        Parameters
        ----------
        start : int
            Entry start.
        stop : int
            Entry stop.

        Returns
        -------
        Chunk
            A sliced :meth:`copy` of ``self`` from ``start`` to ``stop`` with :data:`offset` applied.
        """
        start += self.offset
        stop += self.offset
        invalid = start < self.entry_start or stop < start
        if self._entry_stop is not ...:
            invalid |= stop > self._entry_stop
        if invalid:
            raise ValueError(
                f'Invalid slice: [{start},{stop}) \u2284 [{self.entry_start},{self._entry_stop})')
        chunk = self.copy()
        chunk.entry_start = start
        chunk._entry_stop = stop
        return chunk

    def id(self):
        """
        Returns
        -------
        Chunk
            A :meth:`copy` of ``self`` removing :data:`branches`, :data:`entry_start` and :data:`entry_stop`.
        """
        return Chunk(
            source=(self.path, self.uuid),
            name=self.name,
            num_entries=self.num_entries,
            branches=())

    @classmethod
    def from_path(cls, *paths: str):
        """
        Create :class:`Chunk` from ``paths`` and fetch metadata in parallel.

        Parameters
        ----------
        paths : tuple[str]
            Path to ROOT file.

        Returns
        -------
        list[Chunk]
            List of chunks from ``paths``.
        """
        chunks = [Chunk(path) for path in paths]
        threads = []
        for chunk in chunks:
            thread = Thread(target=chunk._fetch)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        return chunks

    @classmethod
    def partition(cls, size: int, *chunks: Chunk):
        """
        Partition ``chunks`` into groups. The sum of entries in each group is equal to ``size`` except for the last one. The order of chunks is preserved.

        Parameters
        ----------
        size : int
            Size of each group.
        chunks : tuple[Chunk]
            Chunks to partition.

        Yields
        ------
        list[Chunk]
            A group of chunks with total entries equal to ``size``.
        """
        i, start, remain = 0, 0, size
        group: list[Chunk] = []
        while i < len(chunks):
            chunk = min(remain, len(chunks[i]) - start)
            group.append(chunks[i].slice(start, start + chunk))
            remain -= chunk
            start += chunk
            if remain == 0:
                yield group
                group = []
                remain = size
            if start == len(chunks[i]):
                i += 1
                start = 0
        if group:
            yield group


class Friend:  # TODO
    """
    A tool to create and manage addtional :class:`TBranch` stored in separate ROOT files. (also known as friend :class:`TTree`)
    """

    def __init__(self):
        ...  # TODO

    def add(
        self,
        tree: Chunk,
        data: Chunk | ak.Array
    ):
        ...  # TODO

    def get(self):
        ...  # TODO

    def dump(
        self,
        name: str,
        path: PathLike = ...,
    ):
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
