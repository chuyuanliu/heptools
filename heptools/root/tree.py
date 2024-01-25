# TODO
from __future__ import annotations

import bisect
from collections import defaultdict
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
        if isinstance(branches, Iterable):
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
                        f'The number of entries in the file {num_entries} does not match the stored one {self._num_entries}')

                uuid = file.file.uuid
                if self._uuid is ...:
                    self._uuid = uuid
                elif self._uuid != uuid:
                    raise ValueError(
                        f'The UUID of the file {uuid} does not match the stored one {self._uuid}')

    def __hash__(self):
        return hash((self.uuid, self.name))

    def __eq__(self, other):
        if isinstance(other, Chunk):
            return (self.uuid, self.name) == (other.uuid, other.name)
        return NotImplemented

    def __len__(self):
        return self.entry_stop - self.entry_start

    def __repr__(self):
        tree = f'TTree:{self.path}'
        if self._uuid is not ...:
            tree += f'({self._uuid})'
        tree += f':{self.name}'
        if self._entry_stop is not ...:
            tree += f'[{self.entry_start},{self._entry_stop})'
        if self._num_entries is not ...:
            tree += f' \u2286 [0,{self._num_entries})'
        return tree

    def __json__(self):
        return {
            'path': str(self.path),
            'uuid': str(self.uuid),
            'name': self.name,
            'branches': list(self.branches) if self.branches else self.branches,
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
                f'The slice is not a subset of the chunk [{start},{stop}) \u2284 [{self.entry_start},{self._entry_stop})')
        chunk = self.copy()
        chunk.entry_start = start
        chunk._entry_stop = stop
        return chunk

    def source(self):
        """
        Returns
        -------
        Chunk
            A :meth:`copy` of ``self`` keeping only :data:`path`, :data:`uuid` and :data:`name`.
        """
        return Chunk(
            source=(self.path, self.uuid),
            name=self.name,
            num_entries=None,
            branches=None,
            entry_start=None,
            entry_stop=None)

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

    @classmethod
    def from_json(cls, data: dict):
        """
        Create :class:`Chunk` from JSON data.

        Parameters
        ----------
        data : dict
            JSON data.

        Returns
        -------
        Chunk
            Chunk from JSON data.
        """
        return cls(
            source=(data['path'], UUID(data['uuid'])),
            name=data['name'],
            branches=data['branches'],
            num_entries=data['num_entries'],
            entry_start=data['entry_start'],
            entry_stop=data['entry_stop'])

    @classmethod
    def from_coffea_processor(cls, events):
        """
        Create :class:`Chunk` from the input of :meth:`coffea.processor.ProcessorABC.process`.
        """
        metadata = events.metadata
        return cls(
            source=(metadata['filename'], UUID(metadata['fileuuid'])),
            name=metadata['treename'],
            entry_start=metadata['entrystart'],
            entry_stop=metadata['entrystop'])


class _FriendItem:  # TODO
    def __init__(self, start: int, end: int, chunk: Chunk):
        self.start = start
        self.end = end
        self.chunk = chunk

    def __lt__(self, other):
        if isinstance(other, _FriendItem):
            return self.end <= other.start
        return NotImplemented

    def __repr__(self):
        return f'[{self.start},{self.end})'

    def __json__(self):
        return {
            'start': self.start,
            'end': self.end,
            'chunk': self.chunk,
        }

    @classmethod
    def from_json(cls, data: dict):
        return cls(data['start'], data['end'], data['chunk'])


class Friend:  # TODO
    """
    A tool to create and manage addtional :class:`TBranch` stored in separate ROOT files. (also known as friend :class:`TTree`)
    """

    def __init__(self):  # TODO
        self._branches: set[str] = None
        self._data: defaultdict[Chunk, list[_FriendItem]] = defaultdict(list)
        self._to_dump: list[_FriendItem] = []

    def _insert(self, tree: Chunk, item: _FriendItem):
        series = self._data[tree]
        idx = bisect.bisect_left(series, item)
        if idx >= len(series):
            series.append(item)
        else:
            exist = series[idx]
            if item.end <= exist.start:
                series.insert(idx, item)
            else:
                raise ValueError(
                    f'The new chunk {item} overlaps with the existing one {exist} when inserting into {tree}')

    def __add__(self, other):
        if isinstance(other, Friend):
            if self._branches != other._branches:
                raise ValueError(
                    'Cannot merge friend trees with different branches')
        return NotImplemented

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
