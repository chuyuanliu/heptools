from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from functools import partial, reduce
from operator import and_
from typing import TYPE_CHECKING, Iterable
from uuid import UUID

import uproot

from ..logging import log
from ..system.eos import EOS, PathLike
from ..typetools import check_type

if TYPE_CHECKING:
    from logging import Logger


class _ChunkMeta(type):
    def _get(self, attr):
        if getattr(self, attr) is ...:
            self._fetch()
        return getattr(self, attr)

    def __new__(cls, name, bases, dic):
        for attr in ('branches', 'num_entries', 'uuid'):
            dic[attr] = property(partial(cls._get, attr=f'_{attr}'))
        return super().__new__(cls, name, bases, dic)


class Chunk(metaclass=_ChunkMeta):
    """
    A chunk of :class:`TTree` stored in a ROOT file.

    Parameters
    ----------
    source : PathLike or tuple[PathLike, ~uuid.UUID]
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
    '''~heptools.system.eos.EOS : Path to ROOT file.'''
    uuid: UUID
    '''~uuid.UUID : UUID of ROOT file.'''
    name: str
    '''str : Name of :class:`TTree`.'''
    branches: set[str]
    '''set[str] : Name of branches.'''
    num_entries: int
    '''int : Number of entries.'''
    entry_start: int
    '''int : Start entry.'''
    @property
    def entry_stop(self):
        '''int : Stop entry.'''
        if self._entry_stop is ...:
            self._entry_stop = self.num_entries
        return self._entry_stop

    @property
    def offset(self):
        '''int : Equal to ``entry_start``.'''
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

    @classmethod
    def _ignore(cls, value):
        return value is ... or value is None

    def report_integrity(
        self,
        logger: Logger = None
    ):
        """
        Check and report the following:

        - :data:`path` not exists
        - :data:`uuid` different from file
        - :data:`num_entries` different from file
        - :data:`branches` not in file
        - :data:`entry_start` out of range
        - :data:`entry_stop` out of range

        Parameters
        ----------
        logger : ~logging.Logger, optional
            The logger used to report the issues. Can be a :class:`~logging.Logger` any class with the same interface. If not given, the default logger will be used.

        Returns
        -------
        Chunk or None
            A deep copy of ``self`` with corrected metadata. If file not exists, return ``None``.
        """
        if logger is None:
            logger = log
        if not self.path.exists:
            logger.error(f'file not exists')
            return None
        else:
            reloaded = Chunk(
                source=self.path,
                entry_start=self.entry_start,
                entry_stop=self._entry_stop,
                fetch=True)
            if not self._ignore(self._uuid) and self._uuid != reloaded.uuid:
                logger.error(
                    f'UUID mismatch: {self._uuid}(stored) != {reloaded.uuid}(file)')
            if not self._ignore(self._num_entries) and self._num_entries != reloaded.num_entries:
                logger.error(
                    f'number of entries mismatch: {self._num_entries}(stored) != {reloaded.num_entries}(file)')
            if not self._ignore(self._branches):
                diff = self._branches - reloaded.branches
                if diff:
                    logger.error(
                        f'branches {diff} not in file')
            out_of_range = False
            if not self._ignore(self.entry_start):
                out_of_range |= self.entry_start < 0 or self.entry_start >= reloaded.num_entries
            else:
                reloaded.entry_start = 0
            if not self._ignore(self._entry_stop):
                out_of_range |= self._entry_stop <= self.entry_start or self._entry_stop > reloaded.num_entries
            else:
                reloaded._entry_stop = reloaded.num_entries
            if out_of_range:
                logger.error(
                    f'invalid entry range [{self.entry_start},{self._entry_stop}) <- [0,{reloaded.num_entries})')
            return reloaded

    def _fetch(self):
        if any(v is ... for v in (self._branches, self._num_entries, self._uuid)):
            with uproot.open(self.path) as file:
                tree = file[self.name]
                if self._branches is ...:
                    self._branches = {*tree.keys()}
                if self._num_entries is ...:
                    self._num_entries = tree.num_entries
                if self._uuid is ...:
                    self._uuid = file.file.uuid

    def __hash__(self):
        return hash((self.uuid, self.name))

    def __eq__(self, other):
        if isinstance(other, Chunk):
            return (self.uuid, self.name) == (other.uuid, other.name)
        return NotImplemented

    def __len__(self):
        return self.entry_stop - self.entry_start

    def __repr__(self):
        text = f'TTree:{self.path}'
        if self._uuid is not ...:
            text += f'({self._uuid})'
        text += f':{self.name}'
        if self._entry_stop is not ...:
            text += f'[{self.entry_start},{self._entry_stop})'
        if self._num_entries is not ...:
            text += f'<- [0,{self._num_entries})'
        return text

    def __json__(self):
        json_dict = {
            'path': str(self.path),
            'uuid': str(self.uuid),
            'name': self.name,
        }
        if self.branches is not None:
            json_dict['branches'] = list(self.branches)
        if self.num_entries is not None:
            json_dict['num_entries'] = self.num_entries
        if self.entry_start is not None:
            json_dict['entry_start'] = self.entry_start
        if self.entry_stop is not None:
            json_dict['entry_stop'] = self.entry_stop
        return json_dict

    def deepcopy(self, **kwargs):
        """
        Parameters
        ----------
        **kwargs : dict, optional
            Override ``entry_start``, ``entry_stop`` or ``branches``.

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
            branches=kwargs.get('branches', self._branches),
            entry_start=kwargs.get('entry_start', self.entry_start),
            entry_stop=kwargs.get('entry_stop', self._entry_stop))

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
            A sliced :meth:`deepcopy` of ``self`` from ``start`` + :data:`offset` to ``stop`` + :data:`offset`.
        """
        start += self.offset
        stop += self.offset
        invalid = start < self.entry_start or stop < start
        if self._entry_stop is not ...:
            invalid |= stop > self._entry_stop
        if invalid:
            raise ValueError(
                f'The slice is not a subset of the chunk [{start},{stop}) <- [{self.entry_start},{self._entry_stop})')
        chunk = self.deepcopy(entry_start=start, entry_stop=stop)
        return chunk

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
        with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
            executor.map(Chunk._fetch, chunks)
        return chunks

    @classmethod
    def partition(
        cls,
        size: int,
        *chunks: Chunk,
        common_branches: bool = False
    ):
        """
        Partition ``chunks`` into groups. The sum of entries in each group is equal to ``size`` except for the last one. The order of chunks is preserved.

        Parameters
        ----------
        size : int
            Size of each group.
        chunks : tuple[Chunk]
            Chunks to partition.
        common_branches : bool, optional, default=False
            If ``True``, only common branches of all chunks are kept.

        Yields
        ------
        list[Chunk]
            A group of chunks with total entries equal to ``size``.
        """
        i, start, remain = 0, 0, size
        group: list[Chunk] = []
        if common_branches:
            common = reduce(and_, (chunk.branches for chunk in chunks))
            chunks = [chunk.deepcopy(branches=common) for chunk in chunks]
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
            branches=data.get('branches'),
            num_entries=data.get('num_entries'),
            entry_start=data.get('entry_start'),
            entry_stop=data.get('entry_stop'))

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
