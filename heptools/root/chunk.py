from typing import overload

import uproot

from ..system.eos import PathLike


class Chunk:
    xrootd_timeout: int = 3 * 60

    @property
    def _kwargs(self):
        return {
            'path': self.path,
            'object_cache': None,
            'array_cache': None,
            'timeout': self.xrootd_timeout
        }

    @overload
    def __init__(self, path: PathLike, size: int):
        ...
    @overload
    def __init__(self, path: PathLike, start: int, stop: int):
        ...
    @overload
    def __init__(self, path: PathLike, tree: str):
        ...
    def __init__(self, path: PathLike, first: int, second: int = None):
        self.path = path
        if second is None:
            self.start = 0
            if isinstance(first, str):
                with uproot.open(**self._kwargs) as f:
                    self.stop = f[first].num_entries
            else:
                self.stop = first
        else:
            self.start = first
            self.stop = second

    def __len__(self):
        return self.stop - self.start

    def iterate(self, tree: str, branches = None, step: int = None):
        if step is None:
            step = len(self)
        start = self.start
        while start < self.stop:
            end = min(start + step, self.stop)
            with uproot.open(**self._kwargs) as f:
                yield f[tree].arrays(branches, entry_start = start, entry_stop = end)
            start = end

    def __repr__(self):
        return f'<{self.path}:[{self.start},{self.stop})>'

    @staticmethod
    def split(chunksize: int, *files: tuple[PathLike, int]):
        file, event, remain = 0, 0, chunksize
        chunks = []
        while file < len(files):
            chunk = min(remain, files[file][1] - event)
            chunks.append(Chunk(files[file][0], event, event + chunk))
            remain -= chunk
            event += chunk
            if remain == 0:
                yield chunks
                chunks = []
                remain = chunksize
            if event == files[file][1]:
                file += 1
                event = 0
        if chunks:
            yield chunks