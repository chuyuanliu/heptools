from __future__ import annotations

import json
from typing import Callable, Literal

from ..benchmark.unit import Metric
from ..container import Tree

__all__ = ['File', 'FileList', 'Dataset']

class File(dict):
    def  __init__(self, data: dict = {}):
        super().__init__(data)

    @property
    def path(self):
        return self.get('path', '')

    @property
    def events(self):
        return self.get('nevents', 0)

    @property
    def site(self):
        return self.setdefault('site', [])

class FileList(File):
    def __init__(self, data: dict = {}):
        if data.get('files', []):
            data = data | {'files': [File(file) for file in data.get('files', [])]}
        super().__init__(data)

    def __str__(self): # TODO rich, __repr__
        profile = []
        profile.append(f' [path] {self.path}')
        events = [file.events for file in self.files]
        v, u = Metric.add([sum(events), self.events])
        profile.append(f' [nevents] {v[0]:0.1f}{u[0]}/{v[1]:0.1f}{u[1]} [nfiles] {len(events)}/{self["nfiles"]}')
        return '\n'.join(profile)

    @property
    def files(self) -> list[File]:
        return self.setdefault('files', [])

    def sublist(self, file: Callable[[File], bool] = None):
        sublist = FileList(self)
        sublist['files'] = [i for i in self.files if file is None or file(i)]
        return sublist

    def __iter__(self):
        for file in self.files:
            yield file.path

class Dataset:
    _metadata = ['source', 'dataset', 'year', 'era', 'level']

    def __init__(self) -> None:
        self._tree = Tree[FileList]()

    def __str__(self): # TODO rich, __repr__
        return str(self._tree)

    def update(self,
               source: Literal['Data', 'MC'], dataset: str,
               year: str, era: str,
               level: Literal['PicoAOD', 'NanoAOD', 'MiniAOD'], files: FileList):
        self._tree[source, dataset, year, era, level] = FileList(files)

    def subset(self, filelist: Callable[[FileList], bool] = None, file: Callable[[File], bool] = None, **kwarg: str | list[str]):
        subset = Dataset()
        for meta, entry in self._tree.walk(*(kwarg.get(k) for k in self._metadata)):
            entry = entry.sublist(file)
            if filelist is None or filelist(entry):
                subset.update(*meta, entry)
        return subset

    def __iter__(self):
        yield from self._tree.walk()

    def __or__(self, other: Dataset) -> Dataset:
        if isinstance(other, Dataset):
            dataset = Dataset()
            dataset._tree = self._tree | other._tree
            return dataset
        return NotImplemented

    @property
    def files(self):
        for meta, entry in self:
            for file in entry:
                yield meta, file

    def load(self, path: str):
        self._tree = Tree[FileList]().from_dict(json.load(open(path, 'r')), depth = len(self._metadata), leaf = FileList)
        return self

    def save(self, path: str):
        json.dump(self._tree, open(path, 'w'), indent = 4)

    def split(self):
        pass # TODO