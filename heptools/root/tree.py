# TODO
from __future__ import annotations

import bisect
from collections import defaultdict, deque
from functools import partial, reduce
from operator import and_
from threading import Thread
from typing import TYPE_CHECKING, Iterable, Literal
from uuid import UUID

import uproot

from ..system.eos import EOS, PathLike
from ..typetools import check_type
from . import io as root_io
from ._backend import concat_record, record_backend, slice_record

if TYPE_CHECKING:
    import awkward as ak
    import numpy as np
    import pandas as pd


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
        text = f'TTree:{self.path}'
        if self._uuid is not ...:
            text += f'({self._uuid})'
        text += f':{self.name}'
        if self._entry_stop is not ...:
            text += f'[{self.entry_start},{self._entry_stop})'
        if self._num_entries is not ...:
            text += f' \u2286 [0,{self._num_entries})'
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
        **kwargs: dict, optional
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
                f'The slice is not a subset of the chunk [{start},{stop}) \u2284 [{self.entry_start},{self._entry_stop})')
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


class _FriendItem:
    def __init__(self, start: int, stop: int, chunk: Chunk = None):
        self.start = start
        self.stop = stop
        self.chunk = chunk

    def __lt__(self, other):
        if isinstance(other, _FriendItem):
            return self.stop <= other.start
        return NotImplemented

    def __len__(self):
        return self.stop - self.start

    def __repr__(self):
        return f'[{self.start},{self.stop})'

    def __json__(self):
        return {
            'start': self.start,
            'stop': self.stop,
            'chunk': self.chunk,
        }

    @classmethod
    def from_json(cls, data: dict):
        return cls(data['start'], data['stop'], data['chunk'])


class Friend:
    """
    A tool to create and manage a collection of addtional :class:`TBranch` stored in separate ROOT files. (also known as friend :class:`TTree`)

    Parameters
    ----------
    name : str
        Name of the collection.

    Notes
    -----
    The following special methods are implemented:

    - :meth:`__iadd__`
    - :meth:`__add__`
    - :meth:`__repr__`
    - :meth:`__json__`
    """
    name: str
    '''str:Name of the collection.'''

    def __init__(self, name: str):
        self.name = name
        self._branches: set[str] = None
        self._data: defaultdict[Chunk, list[_FriendItem]] = defaultdict(list)

    def __iadd__(self, other) -> Friend:
        if isinstance(other, Friend):
            msg = 'Cannot merge friend trees with different {attr}'
            if self.name != other.name:
                raise ValueError(
                    msg.format(attr='names'))
            if self._branches != other._branches:
                raise ValueError(
                    msg.format(attr='branches'))
            for k, vs in other._data.items():
                for v in vs:
                    self._insert(k, v)
            return self
        return NotImplemented

    def __add__(self, other) -> Friend:
        if isinstance(other, Friend):
            friend = self.copy()
            friend += other
            return friend
        return NotImplemented

    def __repr__(self):
        text = f'Friend:{self.name}\nTBranches:{self._branches}'
        for k, v in self._data.items():
            text += f'\n{k}:{v}'
        return text

    def __json__(self):
        return {
            'name': self.name,
            'branches': list(self._branches) if self._branches is not None else self._branches,
            'data': [*self._data.items()],
        }

    @classmethod
    def _construct_key(cls, chunk: Chunk):
        return Chunk(
            source=(chunk.path, chunk.uuid),
            name=chunk.name,
            num_entries=None,
            branches=None,
            entry_start=None,
            entry_stop=None)

    @property
    def _need_dump(self):
        if hasattr(self, '_dump'):
            return len(self._dump) > 0
        return False

    def _init_dump(self):
        if not hasattr(self, '_dump'):
            self._dump: list[tuple[Chunk, _FriendItem]] = []
            self._dump_name: dict[Chunk, dict[str, str]] = {}

    def _name_dump(self, target: Chunk, item: _FriendItem):
        if target not in self._dump_name:
            names = {
                'name': self.name,
                'uuid': str(target.uuid),
                'tree': target.name,
            }
            parts = [*target.path.parts]
            if parts[0] in '/\\':
                parts = parts[1:]
            parts[-1] = target.path.stem
            parts = parts[::-1]
            for i in range(len(parts)):
                names[f'path{i}'] = parts[i]
            self._dump_name[target] = names
        return self._dump_name[target] | {'start': item.start, 'stop': item.stop}

    def _check_item(self, item: _FriendItem):
        if len(item.chunk) != len(item):
            raise ValueError(
                f'The number of entries in the chunk {item.chunk} does not match the range {item}')
        if self._branches is None:
            self._branches = item.chunk.branches.copy()
        else:
            diff = self._branches - item.chunk.branches
            if diff:
                raise ValueError(
                    f'The chunk {item.chunk} does not have the following branches: {diff}')

    def _insert(self, target: Chunk, item: _FriendItem):
        series = self._data[target]
        idx = bisect.bisect_left(series, item)
        if idx >= len(series):
            series.append(item)
        else:
            exist = series[idx]
            if item.stop <= exist.start:
                series.insert(idx, item)
            else:
                raise ValueError(
                    f'The new chunk {item} overlaps with the existing one {exist} when inserting into {target}')

    def add(
        self,
        target: Chunk,
        data: root_io.RecordLike | Chunk,
    ):
        """
        Create a friend :class:`TTree` for ``target`` using ``data``.

        Parameters
        ----------
        target : Chunk
            A chunk of :class:`TTree`.
        data : RecordLike | Chunk
            Addtional branches added to ``target``.
        """
        item = _FriendItem(target.entry_start, target.entry_stop, data)
        key = self._construct_key(target)
        if not isinstance(data, Chunk):  # TODO test
            self._init_dump()
            self._dump.append((key, item))
        else:
            self._check_item(item)
        self._insert(key, item)

    def get(
        self,
        target: Chunk,
        library: Literal['ak', 'pd', 'np'] = 'ak',
    ) -> root_io.RecordLike:
        """
        Get the friend :class:`TTree` for ``target``.

        Parameters
        ----------
        target : Chunk
            A chunk of :class:`TTree`.
        library : ~typing.Literal['ak', 'np', 'pd'], optional, default='ak'
            The library used to represent arrays.
        """
        series = self._data[target]
        start = target.entry_start
        stop = target.entry_stop
        chunks = deque()
        for i in range(bisect.bisect_left(series, _FriendItem(target.entry_start, target.entry_stop)), len(series)):
            if start >= stop:
                break
            item = series[i]
            if item.start > start:
                raise ValueError(
                    f'Friend {self.name} does not have the entries [{start},{item.start}) for {target}')
            else:
                chunk_start = start - item.start
                start = min(stop, item.stop)
                chunk_end = start - item.start
                if isinstance(item.chunk, Chunk):
                    chunks.append(item.chunk.slice(chunk_start, chunk_end))
                else:
                    backend = record_backend(item.chunk)
                    if backend != library:
                        raise ValueError(
                            f'Data in {item} does not match library={library}')
                    chunks.append(slice_record(
                        item.chunk, chunk_start, chunk_end, library=library))
        # TODO test below
        data = []
        to_read = []
        reader = root_io.TreeReader(self._branches.__and__)
        while chunks:
            chunk = chunks.popleft()
            if isinstance(chunk, Chunk):
                to_read.append(chunk)
            else:
                if to_read:
                    data.append(reader.concat(*to_read, library=library))
                    to_read = []
                data.append(chunk)
        return concat_record(data, library=library)

    def Array(
        self,
        target: Chunk,
    ) -> ak.Array:
        """
        Get the friend :class:`TTree` for ``target`` in :class:`ak.Array`. Equivalent to :meth:`get` with ``library='ak'``.
        """
        return self.get(target, library='ak')

    def DataFrame(
        self,
        target: Chunk,
    ) -> pd.DataFrame:
        """
        Get the friend :class:`TTree` for ``target`` in :class:`pandas.DataFrame`. Equivalent to :meth:`get` with ``library='pd'``.
        """
        return self.get(target, library='pd')

    def NDArray(
        self,
        target: Chunk,
    ) -> dict[str, np.ndarray]:
        """
        Get the friend :class:`TTree` for ``target`` in :class:`dict` of :class:`numpy.ndarray`. Equivalent to :meth:`get` with ``library='np'``.
        """
        return self.get(target, library='np')

    def dump(
        self,
        naming: str,
        basepath: PathLike = ...,
    ):
        """
        Dump all in-memory data to ROOT files with a given ``naming`` rule.

        Parameters
        ----------
        naming : str
            Naming rule for each chunk. See below for details.
        basepath: PathLike, optional
            Base path to store all dumped files. See below for details.

        Notes
        -----
        Each dumped file will be stored in ``{basepath}/{naming.format{**keys}}``. If ``basepath`` is not given, the corresponding ``target.path.parent`` will be used. The following keys are available:

        - ``{name}``: :data:`name`.
        - ``{uuid}``: ``target.uuid``
        - ``{tree}``: ``target.name``
        - ``{start}``: ``target.entry_start``
        - ``{stop}``: ``target.entry_stop``
        - ``{path0}``, ``{path1}``, ... : ``target.path.parts`` without suffixes in reversed order.

        where the ``target`` is the one passed to :meth:`add`.

        .. warning::
            The generated path is not guaranteed to be unique. If multiple chunks are dumped to the same path, the last one will overwrite the previous ones.

        Examples
        --------
        The naming rule works as follows:

        .. code-block:: python

            >>> friend = Friend('test')
            >>> friend.add(
            >>>     Chunk(
            >>>         source=('root://host.1//a/b/c/target.root', uuid),
            >>>         name='Events',
            >>>         entry_start=100,
            >>>         entry_stop=200,
            >>>     ),
            >>>     data
            >>> )
            >>> friend.dump(
            >>>     '{name}_{path0}_{uuid}_{tree}_{start}_{stop}.root')
            >>> # write to root://host.1//a/b/c/test_target_uuid_Events_100_200.root
            >>> # or
            >>> friend.dump(
            >>>     '{path2}/{path1}/{name}_{uuid}_{start}_{stop}.root',
            >>>     'root://host.2//x/y/z/')
            >>> # write to root://host.2//x/y/z/b/c/test_uuid_100_200.root
            """
        ...  # TODO

    def merge(self):
        ...  # TODO

    def move_files(self):
        ...  # TODO

    @classmethod
    def from_json(cls, data: dict):  # TODO
        ...

    def copy(self):
        """
        Returns
        -------
        Friend
            A shallow copy of ``self``. 
        """
        friend = Friend(self.name)
        friend._branches = self._branches.copy()
        for k, vs in self._data.items():
            friend._data[k] = vs.copy()
        return friend


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
