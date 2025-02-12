from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Generator, Iterable, Literal, overload

from ._backend import NameMapping, apply_naming, merge_record, rename_record
from .chunk import Chunk
from .io import BRANCH_FILTER, ReaderOptions, TreeReader

if TYPE_CHECKING:
    import awkward as ak
    import dask.array as da
    import dask_awkward as dak
    import numpy as np
    import pandas as pd

    from .friend import Friend
    from .io import DelayedRecordLike, RecordLike


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

        If the `renaming` function returns a tuple, the data will be stored in a nested record.
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
        return apply_naming(self._rename[friend], {"friend": friend, "branch": branch})

    def _filter(
        self, chunks: tuple[Chunk], reader_options: ReaderOptions, friend_only: bool
    ) -> tuple[dict[str, ReaderOptions], ReaderOptions]:
        output = set()
        b_filter = reader_options.get(BRANCH_FILTER, lambda x: x)
        b_friends: list[tuple[str, set[str], dict[str, str] | None]] = []
        for name, friend in self._friends.items():
            branches = b_filter(friend.branches)
            renames = None
            if name in self._rename:
                renames = {}
                for branch in sorted(branches):
                    renames[self._rename_wrapper(branch, name)] = branch
                branches = set(renames)
            b_friends.append((name, branches, renames))
            output.update(branches)
        if not friend_only:
            b_main = b_filter(Chunk.common(*chunks)[0].branches)
            output.update(b_main)
        opt_friends = {}
        for name, branches, renames in b_friends[::-1]:
            branches = branches.intersection(output)
            if not branches:
                opt_friends[name] = None
            else:
                output.difference_update(branches)
                if renames is not None:
                    branches = {renames[b] for b in branches}
                opt = reader_options.copy()
                opt[BRANCH_FILTER] = branches.intersection
                opt_friends[name] = opt
        opt_main = None
        if not friend_only:
            branches = b_main.intersection(output)
            if branches:
                opt_main = reader_options.copy()
                opt_main[BRANCH_FILTER] = branches.intersection
        return opt_friends, opt_main

    def _fetch(
        self,
        *chunks: Chunk,
        library: Literal["ak", "pd", "np"],
        reader_options: ReaderOptions,
        friend_only: bool = False,
    ) -> RecordLike:
        opt_friends, opt_main = self._filter(chunks, reader_options or {}, friend_only)
        friends = {}
        for name, friend in self._friends.items():
            opt = opt_friends[name]
            if opt is not None:
                data = friend.concat(
                    *chunks,
                    library=library,
                    reader_options=opt,
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
        if friend_only or (opt_main is None):
            return merge_record([*friends.values()], library=library)
        else:
            main = TreeReader(**opt_main).concat(*chunks, library=library)
            return merge_record([main, *friends.values()], library=library)

    @overload
    def concat(
        self,
        library: Literal["ak"] = "ak",
        reader_options: ReaderOptions = None,
        friend_only: bool = False,
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
        )

    @overload
    def iterate(
        self,
        step: int = ...,
        library: Literal["ak"] = "ak",
        mode: Literal["balance", "partition"] = "partition",
        reader_options: ReaderOptions = None,
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
        # TODO predict branches to read
        # TODO add friend_only option
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
