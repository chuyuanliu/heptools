from __future__ import annotations

import json
from typing import Callable, Iterable, Literal

from .benchmark.unit import Metric
from .container import Tree
from .system.cluster.sites import Sites
from .system.eos import EOS, PathLike
from .typetools import DefaultEncoder, alias
from .utils import arg_new

__all__ = ['File', 'FileList', 'Dataset',
           'DatasetError']

class DatasetError(Exception):
    __module__ = Exception.__module__

class File:
    priority: Sites = None

    def  __init__(self,
                  data: dict | File = None,
                  site: str | list[str] = None,
                  path: PathLike = None,
                  nevents: int = None):
        data = arg_new(data, dict)
        self.excluded = False
        if isinstance(site, str):
            site = [site]
        if isinstance(data, File):
            self.excluded = data.excluded
            data = data.__dict__
        self.site = frozenset(data.get('site', []) if site is None else site)
        self.path = data.get('path', '') if path is None else path
        self.nevents = data.get('nevents', 0) if nevents is None else nevents

    def __json__(self):
        return {'path': self.path, 'nevents': self.nevents, 'site': [*self.site]}

    @property
    def eos(self):
        if self.priority is None:
            raise DatasetError('site priority is not specified')
        return EOS(self.path, self.priority.find(self.site))

@alias('copy')
class FileList:
    def __init__(self, data: dict = None):
        data = arg_new(data, dict)
        files = [File(f) for f in data.get('files', [])]
        self._files = {f.path: f for f in files}

    @property
    def files(self) -> Iterable[File]:
        return self._files.values()

    def copy(self):
        return FileList({'files': self.files})

    def __json__(self):
        return {'files': [*self.files]}

    @property
    def nfiles(self) -> tuple[int, int]:
        return len([f for f in self.files if not f.excluded]), len(self.files)

    @property
    def nevents(self) -> tuple[int, int]:
        return sum(f.nevents for f in self.files if not f.excluded), sum(f.nevents for f in self.files)

    def sublist(self, file: Callable[[File], bool] = None):
        sub = self.copy()
        if file is not None:
            for f in sub:
                if not file(f):
                    f.excluded = True
        return sub

    def __add__(self, other: FileList | File) -> FileList:
        if isinstance(other, File):
            other = FileList({'files': [other]})
        if isinstance(other, FileList):
            merged = self.copy()
            for f in other.files:
                if f.path in merged._files:
                    exist = merged._files[f.path]
                    if f.nevents != exist.nevents:
                        raise DatasetError(f'conflicting nevents {f.nevents} vs {exist.nevents} for file "{f.path}"')
                    exist.site = exist.site.union(f.site)
                    exist.excluded = exist.excluded and f.excluded
                else:
                    merged._files[f.path] = File(f)
            return merged
        return NotImplemented

    def __iter__(self):
        for f in self.files:
            if not f.excluded:
                yield f

    def __str__(self): # TODO rich, __repr__
        v, u = Metric.add(self.nevents)
        nf = self.nfiles
        return f'[nevents] {v[0]:0.1f}{u[0]}/{v[1]:0.1f}{u[1]} [nfiles] {nf[0]}/{nf[1]}'

    def reset(self):
        for f in self.files:
            f.excluded = False
        return self

class Dataset:
    _metadata = ('source', 'dataset', 'year', 'era', 'tier')

    def __init__(self) -> None:
        self._tree = Tree(FileList)

    def __str__(self): # TODO rich, __repr__
        return str(self._tree)

    def update(self,
               source: Literal['Data', 'MC'], dataset: str,
               year: str, era: str,
               tier: str, files: FileList):
        self._tree[source, dataset, year, era, tier] += files.copy()

    def subset(self, filelist: Callable[[FileList], bool] = None, file: Callable[[File], bool] = None, **kwarg: str | list[str]):
        subset = Dataset()
        for meta, entry in self._tree.walk(*(kwarg.get(k, ...) for k in self._metadata)):
            entry = entry.sublist(file)
            if filelist is None or filelist(entry):
                subset.update(*meta, entry)
        return subset

    def __iter__(self):
        yield from self._tree.walk()

    def __add__(self, other: Dataset) -> Dataset:
        if isinstance(other, Dataset):
            dataset = Dataset()
            dataset._tree = self._tree + other._tree
            return dataset
        return NotImplemented

    @property
    def files(self):
        for meta, entry in self:
            for file in entry:
                yield meta, file

    @classmethod
    def load(cls, path = None):
        self = cls()
        self._tree.from_dict(json.load(open(path, 'r')), depth = len(self._metadata))
        return self

    def save(self, path: str):
        json.dump(self._tree, open(path, 'w'), indent = 4, cls = DefaultEncoder)

    def reset(self):
        for _, entry in self:
            entry.reset()
        return self