from __future__ import annotations

import bisect
import logging
from collections import defaultdict, deque
from concurrent.futures import Executor, Future
from dataclasses import dataclass
from functools import partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Callable,
    Generator,
    Iterable,
    Literal,
    Optional,
    Protocol,
    overload,
)

from ..dask.delayed import delayed
from ..system.eos import EOS, PathLike
from ..utils import map_executor
from ._backend import merge_record, rename_record
from .chunk import Chunk
from .io import ReaderOptions, TreeReader, TreeWriter, WriterOptions
from .merge import move, resize

if TYPE_CHECKING:
    import awkward as ak
    import dask.array as da
    import dask_awkward as dak
    import numpy as np
    import pandas as pd

    from .io import DelayedRecordLike, RecordLike

_NAMING = "{name}_{uuid}_{start}_{stop}.root"
_FRIEND_AUTO = "_Friend__auto"
_FRIEND_DUMP = "_Friend__dump"
_FRIEND_DISK_ERROR = 'Cannot perform "{func}()" on friend tree "{name}" when there is in-memory data. Call "dump()" first.'
_FRIEND_MISSING_ERROR = 'Missing "{name}" friend entries{range} for {target}.'
_BRANCH_FILTER = "branch_filter"


class NameMapping(Protocol):
    def __call__(self, **keys: str) -> str: ...


def _apply_naming(naming: str | NameMapping, keys: dict[str, str]) -> str:
    if isinstance(naming, str):
        return naming.format(**keys)
    elif isinstance(naming, Callable):
        return naming(**keys)
    else:
        raise TypeError(f'Unknown naming "{naming}"')


@delayed
def _friend_from_merge(
    name: str,
    branches: frozenset[str],
    data: dict[Chunk, list[tuple[int, int, list[Chunk]]]],
    dask: bool,
):
    friend = Friend(name)
    friend._branches = frozenset(branches)
    for k, vs in data.items():
        for start, stop, chunks in vs:
            for chunk in chunks:
                chunk._branches = friend._branches
                friend._data[k].append(_FriendItem(start, start + len(chunk), chunk))
                start += len(chunk)
            if start != stop:
                raise RuntimeError(
                    f'Failed to merge friend "{name}". The merged chunk does not cover the range [{start},{stop}) for target {k}.'
                )
    return friend


@dataclass
class _friend_dump_job:
    path: EOS
    writer: dict
    data: RecordLike

    def __call__(self):
        with TreeWriter(**self.writer)(self.path) as f:
            f.extend(self.data)
        return f.tree


@dataclass
class _friend_dump_callback:
    friend: Friend
    item: _FriendItem

    def __call__(self, tree: Chunk | Future[Chunk]):
        self.item.chunk = tree.result() if isinstance(tree, Future) else tree
        self.friend._check_item(self.item)


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
        return f"[{self.start},{self.stop})"

    def to_json(self):
        return {
            "start": self.start,
            "stop": self.stop,
            "chunk": self.chunk.deepcopy(branches=None),
        }

    @classmethod
    def from_json(cls, data: dict):
        return cls(data["start"], data["stop"], Chunk.from_json(data["chunk"]))

    @property
    def on_disk(self):
        return isinstance(self.chunk, Chunk)


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

    - :meth:`__iadd__` :class:`Friend`
    - :meth:`__add__` :class:`Friend`
    - :meth:`__repr__`
    - :meth:`__enter__`: See :meth:`auto_dump`.
    - :meth:`__exit__`: See :meth:`auto_dump`.
    """

    name: str
    """str : Name of the collection."""

    @property
    def branches(self):
        """
        frozenset[str]: All branches in the friend tree.
        """
        return self._branches

    @property
    def targets(self):
        """
        Generator[:class:`~Chunk`]: All contiguous target chunks.
        """
        for target, chunks in self._contiguous_chunks():
            yield target.slice(chunks[0].start, chunks[-1].stop)

    def _on_disk(func):
        def wrapper(self: Friend, *args, **kwargs):
            if self._has_dump:
                raise RuntimeError(
                    _FRIEND_DISK_ERROR.format(func=func.__name__, name=self.name)
                )
            return func(self, *args, **kwargs)

        return wrapper

    def __init__(self, name: str):
        self.name = name
        self._branches: frozenset[str] = None
        self._data: defaultdict[Chunk, list[_FriendItem]] = defaultdict(list)

    @_on_disk
    def __iadd__(self, other) -> Friend:
        if isinstance(other, Friend):
            msg = "Cannot add friend trees with different {attr}"
            if other._has_dump:
                raise RuntimeError(
                    _FRIEND_DISK_ERROR.format(func="__add__", name=other.name)
                )
            if self.name != other.name:
                raise ValueError(msg.format(attr="names"))
            if self._branches != other._branches:
                raise ValueError(msg.format(attr="branches"))
            for k, vs in other._data.items():
                for v in vs:
                    self._insert(k, v)
            return self
        return NotImplemented

    @_on_disk
    def __add__(self, other) -> Friend:
        if isinstance(other, Friend):
            friend = self.copy()
            friend += other
            return friend
        return NotImplemented

    def __repr__(self):
        text = f"{self.name}:{self._branches}"
        for k, v in self._data.items():
            text += f"\n{k}\n    {v}"
        return text

    @property
    def _auto_dump(self):
        if hasattr(self, _FRIEND_AUTO):
            return self.__auto[0]
        return False

    def auto_dump(
        self,
        base_path: PathLike = ...,
        naming: str | NameMapping = ...,
        writer_options: WriterOptions = None,
        executor: Executor = None,
    ):
        """
        Automatically dump the in-memory data when :meth:`add` is called. The parameters are the same as :meth:`dump`.

        Notes
        -----
        Enable the auto-dump mode by using the with statement:

        .. code-block:: python

            >>> with friend.auto_dump():
            >>>     ...
            >>>     friend.add(target, data)
            >>>     ...
        """
        self.__auto = False, {
            "base_path": base_path,
            "naming": naming,
            "writer_options": writer_options,
            "executor": executor,
        }
        return self

    def __enter__(self):
        if hasattr(self, _FRIEND_AUTO):
            self.__auto = True, self.__auto[1]
        return self

    def __exit__(self, *_):
        if hasattr(self, _FRIEND_AUTO):
            del self.__auto

    @classmethod
    def _construct_key(cls, chunk: Chunk):
        return Chunk(
            source=(chunk.path, chunk.uuid),
            name=chunk.name,
            num_entries=None,
            branches=None,
            entry_start=None,
            entry_stop=None,
        )

    @property
    def _has_dump(self):
        if hasattr(self, _FRIEND_DUMP):
            return len(self.__dump) > 0
        return False

    def _init_dump(self):
        if not hasattr(self, _FRIEND_DUMP):
            self.__dump: list[tuple[Chunk, _FriendItem]] = []
            self.__naming: dict[Chunk, dict[str, str]] = {}

    @classmethod
    def _path_parts(cls, path: EOS, format="path{}") -> list[str]:
        parts = path.parts
        return dict(zip(map(format.format, range(len(parts))), parts[::-1]))

    def _name_dump(self, target: Chunk, item: _FriendItem):
        if target not in self.__naming:
            names = {
                "name": self.name,
                "uuid": str(target.uuid),
                "tree": target.name,
            }
            self.__naming[target] = names | self._path_parts(target.path)
        return self.__naming[target] | {"start": item.start, "stop": item.stop}

    def _check_item(self, item: _FriendItem):
        if len(item.chunk) != len(item):
            raise ValueError(
                f"The number of entries in the chunk {item.chunk} does not match the range {item}"
            )
        if self._branches is None:
            self._branches = frozenset(item.chunk.branches)
        else:
            diff = self._branches - item.chunk.branches
            if diff:
                raise ValueError(
                    f"The chunk {item.chunk} does not have the following branches: {diff}"
                )
        item.chunk = item.chunk.deepcopy(branches=self._branches)

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
                    f"The new chunk {item} overlaps with the existing one {exist} when inserting into {target}"
                )

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
        if not item.on_disk:
            self._init_dump()
            self.__dump.append((key, item))
            if self._auto_dump:
                self.dump(**self.__auto[1])
        else:
            self._check_item(item)
        self._insert(key, item)

    def _new_reader(self, reader_options: ReaderOptions):
        reader_options = dict(reader_options or {})
        branch_filter = reader_options.pop(_BRANCH_FILTER, None)
        branches = (
            self._branches if branch_filter is None else branch_filter(self._branches)
        )
        reader_options[_BRANCH_FILTER] = branches.intersection
        return TreeReader(**reader_options)

    def _match_chunks(self, target: Chunk) -> Generator[Chunk, None, None]:
        if target not in self._data:
            raise ValueError(
                _FRIEND_MISSING_ERROR.format(name=self.name, range="", target=target)
            )
        series = self._data[target]
        start = target.entry_start
        stop = target.entry_stop
        for i in range(
            bisect.bisect_left(
                series, _FriendItem(target.entry_start, target.entry_stop)
            ),
            len(series),
        ):
            if start >= stop:
                break
            item = series[i]
            if item.start > start:
                raise ValueError(
                    _FRIEND_MISSING_ERROR.format(
                        name=self.name, range=f" [{start},{item.start})", target=target
                    )
                )
            else:
                chunk_start = start - item.start
                start = min(stop, item.stop)
                chunk_stop = start - item.start
                yield item.chunk.slice(chunk_start, chunk_stop)

    @overload
    def arrays(
        self,
        target: Chunk,
        library: Literal["ak"] = "ak",
        reader_options: ReaderOptions = None,
    ) -> ak.Array: ...
    @overload
    def arrays(
        self,
        target: Chunk,
        library: Literal["pd"] = "pd",
        reader_options: ReaderOptions = None,
    ) -> pd.DataFrame: ...
    @overload
    def arrays(
        self,
        target: Chunk,
        library: Literal["np"] = "np",
        reader_options: ReaderOptions = None,
    ) -> dict[str, np.ndarray]: ...
    @_on_disk
    def arrays(
        self,
        target: Chunk,
        library: Literal["ak", "pd", "np"] = "ak",
        reader_options: ReaderOptions = None,
    ) -> RecordLike:
        """
        Fetch the friend :class:`TTree` for ``target`` into array.

        Parameters
        ----------
        target : Chunk
            A chunk of :class:`TTree`.
        library : ~typing.Literal['ak', 'np', 'pd'], optional, default='ak'
            The library used to represent arrays.
        reader_options : dict, optional
            Additional options passed to :class:`~.io.TreeReader`.

        Returns
        -------
        RecordLike
            Data from friend :class:`TTree`.
        """
        return self._new_reader(reader_options).concat(
            *self._match_chunks(target), library=library
        )

    @overload
    def concat(
        self,
        *targets: Chunk,
        library: Literal["ak"] = "ak",
        reader_options: ReaderOptions = None,
    ) -> ak.Array: ...
    @overload
    def concat(
        self,
        *targets: Chunk,
        library: Literal["pd"] = "pd",
        reader_options: ReaderOptions = None,
    ) -> pd.DataFrame: ...
    @overload
    def concat(
        self,
        *targets: Chunk,
        library: Literal["np"] = "np",
        reader_options: ReaderOptions = None,
    ) -> dict[str, np.ndarray]: ...
    @_on_disk
    def concat(
        self,
        *targets: Chunk,
        library: Literal["ak", "pd", "np"] = "ak",
        reader_options: ReaderOptions = None,
    ) -> RecordLike:
        """
        Fetch the friend :class:`TTree` for ``targets`` into one array.

        Parameters
        ----------
        targets : tuple[Chunk]
            One or more chunks of :class:`TTree`.
        library : ~typing.Literal['ak', 'np', 'pd'], optional, default='ak'
            The library used to represent arrays.
        reader_options : dict, optional
            Additional options passed to :class:`~.io.TreeReader`.

        Returns
        -------
        RecordLike
            Concatenated data.
        """
        return self._new_reader(reader_options).concat(
            *chain(*map(self._match_chunks, targets)), library=library
        )

    @overload
    def dask(
        self,
        *targets: Chunk,
        library: Literal["ak"] = "ak",
        reader_options: ReaderOptions = None,
    ) -> dak.Array: ...
    @overload
    def dask(
        self,
        *targets: Chunk,
        library: Literal["np"] = "np",
        reader_options: ReaderOptions = None,
    ) -> dict[str, da.Array]: ...
    @_on_disk
    def dask(
        self,
        *targets: Chunk,
        library: Literal["ak", "np"] = "ak",
        reader_options: ReaderOptions = None,
    ) -> DelayedRecordLike:
        """
        Fetch the friend :class:`TTree` for ``targets`` as delayed arrays. The partitions will be preserved.

        Parameters
        ----------
        targets : tuple[Chunk]
            Partitions of target :class:`TTree`.
        library : ~typing.Literal['ak', 'np'], optional, default='ak'
            The library used to represent arrays.
        reader_options : dict, optional
            Additional options passed to :class:`~.io.TreeReader`.

        Returns
        -------
        DelayedRecordLike
            Delayed arrays of entries from the friend :class:`TTree`.
        """
        friends = []
        for target in targets:
            series = self._data[target]
            start = target.entry_start
            stop = target.entry_stop
            item = series[
                bisect.bisect_left(
                    series, _FriendItem(target.entry_start, target.entry_stop)
                )
            ]
            if item.start > start:
                raise ValueError(
                    f"Friend {self.name} does not have the entries [{start},{item.start}) for {target}"
                )
            elif item.stop < stop:
                raise ValueError(
                    'Cannot read one partition from multiple files. Call "merge()" first.'
                )
            else:
                friends.append(item.chunk.slice(start - item.start, stop - item.start))
        return self._new_reader(reader_options).dask(*friends, library=library)

    def dump(
        self,
        base_path: PathLike = ...,
        naming: str | NameMapping = ...,
        writer_options: WriterOptions = None,
        executor: Executor = None,
    ):
        """
        Dump all in-memory data to ROOT files with a given ``naming`` format.

        Parameters
        ----------
        base_path: PathLike, optional
            Base path to store the dumped files. See below for details.
        naming : str or ~typing.Callable, default="{name}_{uuid}_{start}_{stop}.root"
            Naming format for the dumped files. See below for details.
        writer_options : dict, optional
            Additional options passed to :class:`~.io.TreeWriter`.
        executor: ~concurrent.futures.Executor, optional
            An executor with at least the :meth:`~concurrent.futures.Executor.submit` method implemented. If not provided, the tasks will run sequentially in the current thread.

        Notes
        -----
        Each dumped file will be stored in ``{base_path}/{naming.format{**keys}}``. If ``base_path`` is not given, the corresponding ``target.path.parent`` will be used. The following keys are available:

        - ``{name}``: :data:`name`.
        - ``{uuid}``: ``target.uuid``
        - ``{tree}``: ``target.name``
        - ``{start}``: ``target.entry_start``
        - ``{stop}``: ``target.entry_stop``
        - ``{path0}``, ``{path1}``, ... : ``target.path.parts`` in reversed order.

        where the ``target`` is the one passed to :meth:`add`. To apply operations beyond the built-in :meth:`str.format` syntax, use a :data:`~typing.Callable` instead.

        .. warning::
            The generated path is not guaranteed to be unique. If multiple chunks are dumped to the same path, the last one will overwrite the previous ones.

        Examples
        --------
        The naming format works as follows:

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
            >>> # write to root://host.1//a/b/c/test_uuid_Events_100_200_target.root
            >>> friend.dump(
            >>>     '{name}_{uuid}_{tree}_{start}_{stop}_{path0}')
            >>> # or write to root://host.2//x/y/z/b/c/test_uuid_100_200.root
            >>> friend.dump(
            >>>     '{path2}/{path1}/{name}_{uuid}_{start}_{stop}.root',
            >>>     'root://host.2//x/y/z/')
            >>> # or write to root://host.1//a/b/c/tar_events_100_200.root
            >>> def filename(**kwargs: str) -> str:
            >>>     return f'{kwargs["path0"][:3]}_{kwargs["tree"]}_{kwargs["start"]}_{kwargs["stop"]}.root'.lower()
            >>> friend.dump(filename)
        """
        if self._has_dump:
            if base_path is not ...:
                base_path = EOS(base_path)
            if naming is ...:
                naming = _NAMING
            opts = writer_options or {}
            for target, item in self.__dump:
                if base_path is ...:
                    path = target.path.parent
                else:
                    path = base_path
                path = path / _apply_naming(naming, self._name_dump(target, item))
                job = _friend_dump_job(path, opts, item.chunk)
                callback = _friend_dump_callback(self, item)
                if executor is None:
                    callback(job())
                else:
                    executor.submit(job).add_done_callback(callback)
            self.__dump.clear()

    def reset(self, confirm: bool = True, executor: Optional[Executor] = None):
        """
        Reset the friend tree and delete all dumped files.

        Parameters
        ----------
        confirm : bool, optional, default=True
            Confirm the deletion.
        executor: ~concurrent.futures.Executor, optional
            An executor with at least the :meth:`~concurrent.futures.Executor.map` method implemented. If not provided, the tasks will run sequentially in the current thread

        """
        files: list[EOS] = []
        for vs in self._data.values():
            for v in vs:
                if v.on_disk:
                    files.append(v.chunk.path)
        if confirm:
            logging.info("The following files will be deleted:")
            logging.warning("\n".join(str(f) for f in files))
            confirmation = input(f'Type "{self.name}" to confirm the deletion: ')
            if confirmation != self.name:
                logging.info("Deletion aborted.")
                return
        (map_executor if executor is None else executor.map)(EOS.rm, files)
        self._branches = None
        self._data.clear()
        if hasattr(self, _FRIEND_DUMP):
            del self.__dump
            del self.__naming
        if hasattr(self, _FRIEND_AUTO):
            del self.__auto

    def _contiguous_chunks(self):
        for target, items in self._data.items():
            if not items:
                continue
            items = deque(items)
            chunks = []
            while True:
                item = None
                if items:
                    item = items.popleft()
                    if (not chunks) or (chunks[-1].stop == item.start):
                        chunks.append(item)
                        continue
                yield target, chunks
                chunks = [item]
                if item is None:
                    break

    @_on_disk
    def merge(
        self,
        step: int,
        chunk_size: int = ...,
        base_path: PathLike = ...,
        naming: str | NameMapping = "{name}_{uuid}_{start}_{stop}.root",
        reader_options: ReaderOptions = None,
        writer_options: WriterOptions = None,
        clean: bool = True,
        dask: bool = False,
    ):
        """
        Merge contiguous chunks into a single file.

        Parameters
        ----------
        step : int
            Number of entries to read and write in each iteration step.
        chunk_size : int, optional
            Number of entries in each new chunk. If not given, all entries will be merged into one chunk.
        base_path: PathLike, optional
            Base path to store the merged files. See notes of :meth:`dump` for details.
        naming : str or ~typing.Callable, optional
            Naming format for the merged files. See notes of :meth:`dump` for details.
        reader_options : dict, optional
            Additional options passed to :class:`~.io.TreeReader`.
        writer_options : dict, optional
            Additional options passed to :class:`~.io.TreeWriter`.
        clean : bool, optional, default=True
            If ``True``, clean the original friend chunks after merging.
        dask : bool, optional, default=False
            If ``True``, return a :class:`~dask.delayed.Delayed` object.

        Returns
        -------
        Friend or Delayed
            A new friend tree with the merged chunks.
        """
        if base_path is not ...:
            base_path = EOS(base_path)
        self._init_dump()
        data = defaultdict(list)
        for target, chunks in self._contiguous_chunks():
            base = target.path.parent if base_path is ... else base_path
            dummy = _FriendItem(chunks[0].start, chunks[-1].stop)
            path = base / _apply_naming(naming, self._name_dump(target, dummy))
            if len(chunks) == 1:
                chunks = [move(path, chunks[0].chunk, clean_source=clean, dask=dask)]
            else:
                chunks = resize(
                    path,
                    *(c.chunk for c in chunks),
                    step=step,
                    chunk_size=chunk_size,
                    writer_options=writer_options,
                    reader_options=reader_options,
                    clean_source=clean,
                    dask=dask,
                )
            data[target].append((dummy.start, dummy.stop, chunks))
        return _friend_from_merge(
            name=self.name, branches=self._branches, data=dict(data), dask=dask
        )

    @_on_disk
    def clone(
        self,
        base_path: PathLike,
        naming: str | NameMapping = ...,
        execute: bool = False,
        executor: Optional[Executor] = None,
    ):
        """
        Copy all chunks to a new location.

        Parameters
        ----------
        base_path: PathLike
            Base path to store the cloned files.
        naming : str or ~typing.Callable, optional
            Naming format for the cloned files. See below for details. If not given, will simply replace the common base with ``base_path``.
        execute : bool, optional, default=False
            If ``True``, clone the files immediately.
        executor: ~concurrent.futures.Executor, optional
            An executor with at least the :meth:`~concurrent.futures.Executor.map` method implemented. If not provided, the tasks will run sequentially in the current thread.

        Returns
        -------
        Friend
            A new friend tree with the cloned chunks.

        Notes
        -----
        The naming format is the same as :meth:`dump`, with the following additional keys:

        - ``{source0}``, ``{source1}``, ... : ``source.path.parts`` without suffixes in reversed order.

        where the ``source`` is the chunk to be cloned.
        """
        base_path = EOS(base_path)
        friend = Friend(self.name)
        friend._branches = self._branches
        src = []
        dst = []
        self._init_dump()
        if naming is ...:
            base = EOS.common_base(
                *(c.chunk.path for cs in self._data.values() for c in cs)
            )
        for target, items in self._data.items():
            if not items:
                continue
            for item in items:
                chunk = item.chunk.deepcopy()
                if naming is ...:
                    name = chunk.path.as_local.relative_to(base)
                else:
                    name = _apply_naming(
                        naming,
                        self._name_dump(target, item)
                        | self._path_parts(chunk.path, "source{}"),
                    )
                path = base_path / name
                src.append(chunk.path)
                dst.append(path)
                chunk.path = path
                friend._data[target].append(_FriendItem(item.start, item.stop, chunk))
        if execute:
            (map_executor if executor is None else executor.map)(
                partial(EOS.cp, parents=True, overwrite=True), src, dst
            )
        return friend

    def integrity(self, executor: Executor = None):
        """
        Check and report the following:

        - :meth:`~.chunk.Chunk.integrity` for all target and friend chunks
        - multiple friend chunks from the same source
        - mismatch in number of entries or branches
        - gaps or overlaps between friend chunks
        - in-memory data

        This method can be very expensive for large friend trees.

        Parameters
        ----------
        executor: ~concurrent.futures.Executor, optional
            An executor with at least the :meth:`~concurrent.futures.Executor.map` method implemented. If not provided, the tasks will run sequentially in the current thread.
        """
        checked = []
        files = set()
        for target, items in self._data.items():
            checked.append(target)
            for item in items:
                if item.on_disk:
                    checked.append(item.chunk)
        checked = deque(
            (map if executor is None else executor.map)(Chunk.integrity, checked)
        )
        for target, items in self._data.items():
            target = checked.popleft()
            if target is not None:
                target_name = f'target "{target.path}"\n    '
                start = target.entry_start
                stop = target.entry_stop
                for item in items:
                    if start < item.start:
                        logging.warning(
                            f"{target_name}no friend entries in [{start},{item.start})"
                        )
                    elif start > item.start:
                        logging.error(
                            f"{target_name}duplicate friend entries in [{item.start}, {start})"
                        )
                    start = item.stop
                    if item.on_disk:
                        chunk = checked.popleft()
                        if chunk is not None:
                            friend_name = f'friend "{chunk.path}"\n    '
                            if len(chunk) != len(item):
                                logging.error(
                                    f"{friend_name}{len(chunk)} entries not fit in [{item.start},{item.stop})"
                                )
                            diff = self._branches - chunk.branches
                            if diff:
                                logging.error(f"{friend_name}missing branches {diff}")
                            if chunk.path in files:
                                logging.error(
                                    f"{friend_name}multiple friend chunks from this source"
                                )
                            files.add(chunk.path)
                    else:
                        logging.warning(
                            f"{target_name}in-memory friend entries in [{item.start},{item.stop})"
                        )
                if start < stop:
                    logging.warning(
                        f"{target_name}no friend entries in [{start},{stop}) "
                    )
            else:
                for item in items:
                    if item.on_disk:
                        checked.popleft()

    @_on_disk
    def to_json(self):
        """
        Convert ``self`` to JSON data.

        Returns
        -------
        dict
            JSON data.
        """
        return {
            "name": self.name,
            "branches": (
                list(self._branches) if self._branches is not None else self._branches
            ),
            "data": [*self._data.items()],
        }

    @classmethod
    def from_json(cls, data: dict):
        """
        Create :class:`Friend` from JSON data.

        Parameters
        ----------
        data : dict
            JSON data.

        Returns
        -------
        Friend
            A :class:`Friend` object from JSON data.
        """
        friend = cls(data["name"])
        friend._branches = frozenset(data["branches"])
        for k, vs in data["data"]:
            items = []
            for v in vs:
                v["chunk"]["branches"] = friend._branches
                items.append(_FriendItem.from_json(v))
            friend._data[Chunk.from_json(k)] = items
        return friend

    @_on_disk
    def copy(self):
        """
        Returns
        -------
        Friend
            A shallow copy of ``self``.
        """
        friend = Friend(self.name)
        friend._branches = self._branches
        for k, vs in self._data.items():
            friend._data[k] = vs.copy()
        return friend


class Chain:
    """
    A :class:`TChain` like object to manage multiple :class:`~.chunk.Chunk` and :class:`Friend`.

    The structure of output record is given by the following pseudo code:

    - ``library='ak'``:
        .. code-block:: python

            record[main.branch] = array
            record[friend.name][friend.branch] = array

    - ``library='pd', 'np'``:
        .. code-block:: python

            record[main.branch] = array
            record[friend.branch] = array

    If duplicate branches are found after rename, the one in the friend tree that appears last will be kept.

    Notes
    -----
    The following special methods are implemented:

    - :meth:`__iadd__` :class:`~.chunk.Chunk`, :class:`Friend`, :class:`Chain`
    - :meth:`__add__` :class:`Chain`
    """

    def __init__(self):
        self._chunks: list[Chunk] = []
        self._friends: dict[str, Friend] = {}
        self._rename: dict[str, str] = {}

    def add_chunk(self, *chunks: Chunk):
        """
        Add :class:`~.chunk.Chunk` to this chain.

        Parameters
        ----------
        chunks : tuple[Chunk]
            Chunks to add.

        Returns
        -------
        self: Chain
        """
        self._chunks.extend(chunks)
        return self

    def add_friend(
        self,
        *friends: Friend,
        renaming: str | NameMapping = None,
    ):
        """
        Add new :class:`Friend` to this chain or merge to the existing ones.

        Parameters
        ----------
        friends : tuple[Friend]
            Friends to add or merge.
        renaming : str or ~typing.Callable, optional
            If given, the branches in the friend trees will be renamed. See below for available keys.

        Returns
        -------
        self: Chain

        Notes
        -----
        The following keys are available for renaming:

        - ``{friend}``: :data:`Friend.name`
        - ``{branch}``: branch name
        """
        for friend in friends:
            name = friend.name
            if name in self._friends:
                self._friends[name] = self._friends[name] + friend
            else:
                self._friends[name] = friend
        if renaming is not None:
            for friend in friends:
                self._rename[friend.name] = renaming
        return self

    def copy(self):
        """
        Returns
        -------
        Chain
            A shallow copy of ``self``.
        """
        chain = Chain()
        chain._chunks += self._chunks
        chain._friends |= self._friends
        chain._rename |= self._rename
        return chain

    def __iadd__(self, other) -> Chain:
        if isinstance(other, Chunk):
            return self.add_chunk(other)
        elif isinstance(other, Friend):
            return self.add_friend(other)
        elif isinstance(other, Chain):
            self.add_chunk(*other._chunks)
            for name, friend in other._friends.items():
                self.add_friend(friend, renaming=other._rename.get(name))
        elif isinstance(other, Iterable):
            for item in other:
                self += item
        else:
            return NotImplemented
        return self

    def __add__(self, other) -> Chain:
        if isinstance(other, Chain):
            chain = self.copy()
            chain += other
            return chain
        return NotImplemented

    def _rename_wrapper(self, branch: str, friend: str) -> str:
        return _apply_naming(self._rename[friend], {"friend": friend, "branch": branch})

    def _fetch(
        self,
        *chunks: Chunk,
        library: Literal["ak", "pd", "np"],
        reader_options: ReaderOptions,
        friend_only: bool = False,
        awkward_nested: bool = True,
    ) -> RecordLike:
        reader_options = reader_options or {}
        friends = {}
        for name, friend in self._friends.items():
            data = friend.concat(
                *chunks,
                library=library,
                reader_options=reader_options,
            )
            if name in self._rename:
                data = rename_record(
                    data,
                    partial(self._rename_wrapper, friend=name),
                    library=library,
                )
            friends[name] = data
        if library == "ak":
            friends = {k: v for k, v in friends.items() if len(v.fields) > 0}
        if friend_only:
            if library == "ak" and awkward_nested:
                import awkward as ak

                return ak.Array(friends)
            return merge_record([*friends.values()], library=library)
        else:
            main = TreeReader(**reader_options).concat(*chunks, library=library)
            if library == "ak" and awkward_nested:
                for name, friend in friends.items():
                    main[name] = friend
                return main
            return merge_record([main, *friends.values()], library=library)

    @overload
    def concat(
        self,
        library: Literal["ak"] = "ak",
        reader_options: ReaderOptions = None,
        friend_only: bool = False,
        awkward_nested: bool = True,
    ) -> ak.Array: ...
    @overload
    def concat(
        self,
        library: Literal["pd"] = "pd",
        reader_options: ReaderOptions = None,
        friend_only: bool = False,
    ) -> pd.DataFrame: ...
    @overload
    def concat(
        self,
        library: Literal["np"] = "np",
        reader_options: ReaderOptions = None,
        friend_only: bool = False,
    ) -> dict[str, np.ndarray]: ...
    def concat(
        self,
        library: Literal["ak", "pd", "np"] = "ak",
        reader_options: ReaderOptions = None,
        friend_only: bool = False,
        awkward_nested: bool = True,
    ) -> RecordLike:
        """
        Read all chunks and friend trees into one record.

        Parameters
        ----------
        library : ~typing.Literal['ak', 'np', 'pd'], optional, default='ak'
            The library used to represent arrays.
        reader_options : dict, optional
            Additional options passed to :class:`~.io.TreeReader`.
        friend_only : bool, optional, default=False
            If ``True``, only read friend trees.
        awkward_nested: bool, optional, default=True
            If ``True``, the output will be a nested array. Only works for ``library='ak'``.

        Returns
        -------
        RecordLike
            Concatenated data.
        """
        return self._fetch(
            *self._chunks,
            library=library,
            reader_options=reader_options,
            friend_only=friend_only,
            awkward_nested=awkward_nested,
        )

    @overload
    def iterate(
        self,
        step: int = ...,
        library: Literal["ak"] = "ak",
        mode: Literal["balance", "partition"] = "partition",
        reader_options: ReaderOptions = None,
        awkward_nested: bool = True,
    ) -> Generator[ak.Array, None, None]: ...
    @overload
    def iterate(
        self,
        step: int = ...,
        library: Literal["pd"] = "pd",
        mode: Literal["balance", "partition"] = "partition",
        reader_options: ReaderOptions = None,
    ) -> Generator[pd.DataFrame, None, None]: ...
    @overload
    def iterate(
        self,
        step: int = ...,
        library: Literal["np"] = "np",
        mode: Literal["balance", "partition"] = "partition",
        reader_options: ReaderOptions = None,
    ) -> Generator[dict[str, np.ndarray], None, None]: ...
    def iterate(
        self,
        step: int = ...,
        library: Literal["ak", "pd", "np"] = "ak",
        mode: Literal["balance", "partition"] = "partition",
        reader_options: ReaderOptions = None,
        awkward_nested: bool = True,
    ) -> Generator[RecordLike, None, None]:
        """
        Iterate over chunks and friend trees.

        Parameters
        ----------
        step : int, optional
            Number of entries to read in each iteration step. If not given, the chunk size will be used and the ``mode`` will be ignored.
        library : ~typing.Literal['ak', 'np', 'pd'], optional, default='ak'
            The library used to represent arrays.
        mode : ~typing.Literal['balance', 'partition'], optional, default='partition'
            The mode to generate iteration steps. See :meth:`~.io.TreeReader.iterate` for details.
        reader_options : dict, optional
            Additional options passed to :class:`~.io.TreeReader`.
        awkward_nested: bool, optional, default=True
            If ``True``, the output will be a nested array. Only works for ``library='ak'``.

        Yields
        ------
        RecordLike
            A chunk of merged data from main and friend :class:`TTree`.
        """
        if step is ...:
            chunks = Chunk.common(*self._chunks)
        elif mode == "partition":
            chunks = Chunk.partition(step, *self._chunks, common_branches=True)
        elif mode == "balance":
            chunks = Chunk.balance(step, *self._chunks, common_branches=True)
        else:
            raise ValueError(f'Unknown mode "{mode}"')
        for chunk in chunks:
            if not isinstance(chunk, list):
                chunk = (chunk,)
            yield self._fetch(
                *chunk,
                library=library,
                reader_options=reader_options,
                friend_only=False,
                awkward_nested=awkward_nested,
            )

    @overload
    def dask(
        self,
        partition: int = ...,
        library: Literal["ak"] = "ak",
        reader_options: ReaderOptions = None,
    ) -> dak.Array: ...
    @overload
    def dask(
        self,
        partition: int = ...,
        library: Literal["np"] = "np",
        reader_options: ReaderOptions = None,
    ) -> dict[str, da.Array]: ...
    def dask(
        self,
        partition: int = ...,
        library: Literal["ak", "np"] = "ak",
        reader_options: ReaderOptions = None,
    ) -> DelayedRecordLike:
        """
        Read chunks and friend trees into delayed arrays.

        .. warning::
            The ``renaming`` option will be ignored when using ``library='ak'``.

        Parameters
        ----------
        partition: int, optional
            If given, the ``sources`` will be splitted into smaller chunks targeting ``partition`` entries.
        library : ~typing.Literal['ak', 'np'], optional, default='ak'
            The library used to represent arrays.
        reader_options : dict, optional
            Additional options passed to :class:`~.io.TreeReader`.

        Returns
        -------
        main : DelayedRecordLike
            Delayed data from main and friend :class:`TTree`.
        """
        if partition is ...:
            partitions = Chunk.common(*self._chunks)
        else:
            partitions = [
                *Chunk.balance(partition, *self._chunks, common_branches=True)
            ]
        reader_options = reader_options or {}
        main = TreeReader(**reader_options).dask(*partitions, library=library)
        friends = {}
        for name, friend in self._friends.items():
            friend = friend.dask(
                *partitions,
                library=library,
                reader_options=reader_options,
            )
            if library != "ak" and name in self._rename:
                friend = rename_record(
                    friend, partial(self._rename_wrapper, friend=name), library=library
                )
            friends[name] = friend
        if library == "ak":
            for name, friend in friends.items():
                if len(friend.fields) > 0:
                    main[name] = friend
            return main
        else:
            return merge_record([main, *friends.values()], library=library)
