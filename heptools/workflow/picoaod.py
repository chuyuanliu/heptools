from abc import abstractmethod

from coffea.processor import ProcessorABC

from heptools.awkward.zip import NanoAOD
from heptools.root.chunk import Chunk
from heptools.root.io import merge_tree, TreeReader, TreeWriter
from heptools.system.eos import EOS, PathLike
import re
import awkward as ak


class PicoAOD(ProcessorABC):
    def __init__(
            self,
            basepath: PathLike,
            collections: list[str],
            branches: list[str],
            step: int = 50_000):
        self._basepath = EOS(basepath)
        self._step = step
        # TODO select or skip
        selected = [f"{collection}_.*" for collection in collections] + \
            [f"n{collection}" for collection in collections] + branches
        self._filter_branches = re.compile(f'^({"|".join(selected)})$')
        self._transform = NanoAOD(regular=False, jagged=True)

    def _filter(self, branches: set[str]):
        return {*filter(self._filter_branches.match, branches)}

    @abstractmethod
    def select(self, events):
        pass

    def process(self, events):
        selected = self.select(events)
        chunk = Chunk.from_coffea_processor(events)
        # category = events.metadata['category'] # TODO mc, dataset, year
        # is_mc = events.metadata['is_mc'] # TODO check
        category = 'test'  # TODO remove
        result = {category: {
            'nevents': len(events),
        }}
        # TODO output path
        path = self._basepath / \
            f'{category}/picoAOD_{chunk.uuid}_{chunk.entry_start}_{chunk.entry_stop}.root'
        with TreeWriter()(path) as writer:
            for i, data in enumerate(TreeReader(self._filter, self._transform).iterate(self._step, chunk)):
                writer.extend(data[selected[i*self._step:(i+1)*self._step]])
                print(chunk.entry_start, chunk.entry_stop,
                      f'{i*self._step} / {len(chunk)}')
        result[category]['file'] = [writer.tree]

        return result

    def postprocess(self, accumulator):
        pass


def fetch_mc_metadata(**paths: list[PathLike]):
    ...


def resize(
    path: PathLike,
    *chunks: Chunk,
    read_step: int,
    chunk_size: int = ...,
    dask: bool = False,
):
    path = EOS(path)
    transform = NanoAOD(regular=False, jagged=True)
    results: list[Chunk] = []
    if chunk_size is ...:
        results.append(merge_tree(
            path,
            read_step,
            *chunks,
            reader_options={'transform': transform},
            dask=dask))
    else:
        parent = path.parent
        filename = f'{path.stem}.chunk{{index}}{"".join(path.suffixes)}'
        for index, new_chunks in enumerate(Chunk.partition(chunk_size, *chunks)):
            new_path = parent / filename.format(index=index)
            results.append(merge_tree(
                new_path,
                read_step,
                *new_chunks,
                reader_options={'transform': transform},
                dask=dask))
    return results
