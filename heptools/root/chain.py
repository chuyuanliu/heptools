# TODO
from __future__ import annotations

import bisect
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Literal

from ..logging import log
from ..system.eos import EOS, PathLike
from ._backend import concat_record, record_backend, slice_record
from .chunk import Chunk
from .io import TreeReader, TreeWriter

if TYPE_CHECKING:
    from logging import Logger

    import awkward as ak
    import numpy as np
    import pandas as pd

    from .io import RecordLike


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
    '''str : Name of the collection.'''

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
    def _to_dump(self):
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
        data: RecordLike | Chunk,
    ):
        """
        Create a friend :class:`TTree` for ``target`` using ``data``.

        Parameters
        ----------
        target : Chunk
            A chunk of :class:`TTree`.
        data : RecordLike or Chunk
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
        branches: set[str] = ...,
        library: Literal['ak', 'pd', 'np'] = 'ak',
        reader_options: dict = None,
    ) -> RecordLike:
        """
        Get the friend :class:`TTree` for ``target``.

        Parameters
        ----------
        target : Chunk
            A chunk of :class:`TTree`.
        branches : set[str], optional
            Branches to read. If not given, all branches will be read.
        library : ~typing.Literal['ak', 'np', 'pd'], optional, default='ak'
            The library used to represent arrays.
        reader_options : dict, optional
            Additional options passed to :class:`~.io.TreeReader`.
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
        if branches is ...:
            branches = self._branches
        else:
            branches = self._branches & branches
        reader_options = reader_options or {}
        reader_options['filter'] = branches.__and__
        reader = TreeReader(**reader_options)
        data = []
        to_read = []
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
        naming: str = '{name}_{uuid}_{start}_{stop}.root',
        base_path: PathLike = ...,
        writer_options: dict = None,
    ):
        """
        Dump all in-memory data to ROOT files with a given ``naming`` rule.

        Parameters
        ----------
        naming : str
            Naming rule for each chunk. See below for details.
        base_path: PathLike, optional
            Base path to store all dumped files. See below for details.
        writer_options: dict, optional
            Additional options passed to :class:`~.io.TreeWriter`.

        Notes
        -----
        Each dumped file will be stored in ``{base_path}/{naming.format{**keys}}``. If ``base_path`` is not given, the corresponding ``target.path.parent`` will be used. The following keys are available:

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
        if self._to_dump:
            if base_path is not ...:
                base_path = EOS(base_path)
            writer_options = writer_options or {}
            writer = TreeWriter(**writer_options)
            for target, item in self._dump:
                if base_path is ...:
                    path = target.path.parent
                else:
                    path = base_path
                path = path / naming.format(**self._name_dump(target, item))
                with writer(path) as f:
                    f.extend(item.chunk)
                item.chunk = f.tree
                self._check_item(item)
            self._dump.clear()

    def reset(self, confirm: bool = True):
        """
        Reset the friend tree and delete all dumped files.

        Parameters
        ----------
        confirm : bool, optional, default=True
            Confirm the deletion.
        """
        files = []
        for vs in self._data.values():
            for v in vs:
                if isinstance(v.chunk, Chunk):
                    files.append(v.chunk.path)
        if confirm:
            log.info('The following files will be deleted:')
            log.warning('\n'.join(str(f) for f in files))
            confirmation = log.input(
                f'Type "{self.name}" to confirm the deletion: ')
            if confirmation != self.name:
                log.info('Deletion aborted.')
                return
        with ThreadPoolExecutor(max_workers=len(files)) as executor:
            executor.map(EOS.rm, files)
        self._branches = None
        self._data.clear()
        if hasattr(self, '_dump'):
            del self._dump
            del self._dump_name

    def merge(
        self,
        naming: str = '{name}_{uuid}_{start}_{stop}.root',
        base_path: PathLike = ...,
        reader_options: dict = None,
        writer_options: dict = None,
        dask: bool = False,
    ):
        """
        Merge contiguous chunks into a single file.
        """
        ...  # TODO

    def move(self):
        ...  # TODO

    def report_integrity(
        self,
        logger: Logger = None
    ):
        """
        Check and report the following:

        - :meth:`~.chunk.Chunk.report_integrity` for all target and friend chunks
        - mismatch in number of entries or branches
        - gaps or overlaps between friend chunks
        - in-memory data

        This method can be very expensive for large friend trees.

        Parameters
        ----------
        logger : ~logging.Logger, optional
            The logger used to report the issues. Can be a :class:`~logging.Logger` or any class with the same interface. If not given, the default logger will be used.
        """
        if logger is None:
            logger = log
        # TODO check below
        for target, items in self._data.items():
            target = target.report_integrity(logger)
            if target is not None:
                start = target.entry_start
                stop = target.entry_stop
                for item in items:
                    if start < item.start:
                        logger.warning(
                            f'target "{target.path}" does not have the friend entries [{start},{item.start}) ')  # TODO check
                    elif start > item.start:
                        logger.error(
                            f'target "{target.path}" has multiple friends overlapping at [{item.start}, {start})')  # TODO check
                    start = item.stop
                    if isinstance(item.chunk, Chunk):
                        if item.chunk.report_integrity(logger) is not None:
                            chunk = item.chunk
                            if len(chunk) != len(item):
                                logger.error(
                                    f'friend "{chunk}" does not match the range [{item.start},{item.stop})')  # TODO check
                            diff = self._branches - chunk.branches
                            if diff:
                                logger.error(
                                    f'friend "{chunk}" does not have the following branches: {diff}')  # TODO check
                    else:
                        logger.warning(
                            f'target "{target.path}" has the friend entries [{item.start},{item.stop}) in memory.')  # TODO check
                if start < stop:
                    logger.warning(
                        f'target "{target.path}" does not have the friend entries [{start},{stop}) ')  # TODO check

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
    A :class:`TChain` like object to manage multiple :class:`~.chunk.Chunk` and :class:`Friend`.
    """

    def append(self):
        ...  # TODO

    def extend(self):
        ...  # TODO

    def add_friend(self):
        ...  # TODO

    def iterate(self):
        ...  # TODO
