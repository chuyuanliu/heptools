import re
from abc import abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor

import awkward as ak
import uproot
from coffea.processor import ProcessorABC, accumulate

from heptools.awkward.zip import NanoAOD
from heptools.root import Chunk, TreeReader, TreeWriter, merge
from heptools.system.eos import EOS, PathLike

_PICOAOD = 'picoAOD'
_ROOT = '.root'


class PicoAOD(ProcessorABC):
    def __init__(
            self,
            basepath: PathLike,
            selected_collections: list[str],
            selected_branches: list[str],
            step: int = 50_000):
        self._basepath = EOS(basepath)
        self._step = step
        # TODO select or skip
        selected = (
            [f"{collection}_.*" for collection in selected_collections] +
            [f"n{collection}" for collection in selected_collections] +
            selected_branches)
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
        dataset = events.metadata['dataset']
        result = {dataset: {
            'nevents': len(events),
        }}
        filename = f'{dataset}/{_PICOAOD}_{chunk.uuid}_{chunk.entry_start}_{chunk.entry_stop}{_ROOT}'
        path = self._basepath / filename
        with TreeWriter()(path) as writer:
            for i, data in enumerate(TreeReader(self._filter, self._transform).iterate(self._step, chunk)):
                writer.extend(data[selected[i*self._step:(i+1)*self._step]])
        result[dataset]['files'] = [writer.tree]

        return result

    def postprocess(self, accumulator):
        pass


def _fetch_metadata(dataset: str, path: PathLike):
    with uproot.open(path) as f:
        data = f['Runs'].arrays(
            ['genEventCount', 'genEventSumw', 'genEventSumw2'])
        return {
            dataset: {
                'count': ak.sum(data['genEventCount']),
                'sumw': ak.sum(data['genEventSumw']),
                'sumw2': ak.sum(data['genEventSumw2']),
            }
        }


def fetch_metadata(fileset: dict[str, dict[str]]) -> dict[str, dict[str]]:
    count = sum(len(path['files']) for path in fileset.values())
    with ThreadPoolExecutor(max_workers=count) as executor:
        tasks: list[Future] = []
        for dataset, files in fileset.items():
            for files in files['files']:
                tasks.append(executor.submit(_fetch_metadata, dataset, files))
        results = [task.result() for task in tasks]
    return accumulate(results)


def resize(
        basepath: PathLike,
        output: dict[str, dict[str, list[Chunk]]],
        step: int,
        chunk_size: int):
    basepath = EOS(basepath)
    transform = NanoAOD(regular=False, jagged=True)
    for dataset, chunks in output.items():
        files = [chunk.path for chunk in chunks['files']]
        output[dataset][files] = merge.resize(
            basepath / dataset/f'{_PICOAOD}{_ROOT}',
            files,
            step=step,
            chunk_size=chunk_size,
            reader_options={'transform': transform},
            dask=True,
        )
    return output
