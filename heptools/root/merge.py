from ..dask.delayed import delayed
from ..system.eos import EOS, PathLike
from .chunk import Chunk
from .io import TreeReader, TreeWriter


@delayed
def move(
    path: PathLike,
    source: Chunk,
    dask: bool = False,
):
    """
    Move ``source`` to ``path``.

    Parameters
    ----------
    path : PathLike
        Path to output ROOT file.
    source : ~heptools.root.chunk.Chunk
        Source chunk to move.
    dask : bool, optional, default=False
        If ``True``, return a :class:`~dask.delayed.Delayed` object.

    Returns
    -------
    Chunk or Delayed
        Moved chunk.
    """
    source = source.deepcopy()
    source.path = source.path.move_to(path)
    return source


@delayed
def merge(
    path: PathLike,
    *sources: Chunk,
    step: int,
    writer_options: dict = None,
    reader_options: dict = None,
    dask: bool = False,
):
    """
    Merge ``sources`` into one :class:`~.chunk.Chunk`.

    Parameters
    ----------
    path : PathLike
        Path to output ROOT file.
    sources : tuple[~heptools.root.chunk.Chunk]
        Chunks to merge.
    step : int
        Number of entries to read and write in each iteration step.
    writer_options : dict, optional
        Additional options passed to :class:`~.io.TreeWriter`.
    reader_options : dict, optional
        Additional options passed to :class:`~.io.TreeReader`.
    dask : bool, optional, default=False
        If ``True``, return a :class:`~dask.delayed.Delayed` object.

    Returns
    -------
    Chunk or Delayed
        Merged chunk.
    """
    writer_options = writer_options or {}
    reader_options = reader_options or {}
    with TreeWriter(**writer_options)(path) as writer:
        for data in TreeReader(**reader_options).iterate(*sources, step=step):
            writer.extend(data)
    return writer.tree


@delayed
def clean(
    source: list[Chunk],
    merged: list[Chunk],
    dask: bool = False,
):
    """
    Clean ``source`` after merging.

    Parameters
    ----------
    source : list[~heptools.root.chunk.Chunk]
        Source chunks to be cleaned.
    merged : list[~heptools.root.chunk.Chunk]
        Merged chunks.
    dask : bool, optional, default=False
        If ``True``, return a :class:`~dask.delayed.Delayed` object.

    Returns
    -------
    merged: list[Chunk] or Delayed
    """
    for chunk in source:
        chunk.path.rm()
    return merged


def resize(
    path: PathLike,
    *sources: Chunk,
    step: int,
    chunk_size: int = ...,
    writer_options: dict = None,
    reader_options: dict = None,
    dask: bool = False,
):
    """
    :func:`merge` ``sources`` into :class:`~.chunk.Chunk` and :func:`clean` ``sources`` after merging.

    Parameters
    ----------
    path : PathLike
        Path to output ROOT file.
    sources : tuple[~heptools.root.chunk.Chunk]
        Chunks to merge.
    step : int
        Number of entries to read and write in each iteration step.
    chunk_size : int, optional
        Number of entries in each chunk. If not given, all entries will be merged into one chunk.
    writer_options : dict, optional
        Additional options passed to :class:`~.io.TreeWriter`.
    reader_options : dict, optional
        Additional options passed to :class:`~.io.TreeReader`.
    dask : bool, optional, default=False
        If ``True``, return a :class:`~dask.delayed.Delayed` object.

    Returns
    -------
    list[Chunk] or Delayed
        Merged chunks.
    """
    path = EOS(path)
    results: list[Chunk] = []
    if chunk_size is ...:
        results.append(
            merge(
                path,
                *sources,
                step=step,
                reader_options=reader_options,
                writer_options=writer_options,
                dask=dask,
            )
        )
    else:
        parent = path.parent
        filename = f'{path.stem}.chunk{{index}}{"".join(path.suffixes)}'
        chunks = [*Chunk.partition(chunk_size, *sources, common_branches=True)]
        for index, new_chunks in enumerate(chunks):
            if len(chunks) == 1:
                new_path = path
            else:
                new_path = parent / filename.format(index=index)
            results.append(
                merge(
                    new_path,
                    *new_chunks,
                    step=step,
                    reader_options=reader_options,
                    writer_options=writer_options,
                    dask=dask,
                )
            )
    results = clean(sources, results, dask=dask)
    return results
