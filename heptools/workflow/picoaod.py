# TODO migrate to coffea2024
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
        base_path: PathLike,
        step: int,
        skip_collections: list[str] = None,
        skip_branches: list[str] = None,
    ):
        self._base = EOS(base_path)
        self._step = step
        if skip_collections is None:
            skip_collections = []
        if skip_branches is None:
            skip_branches = []
        skipped = (
            [f'{collection}_.*' for collection in skip_collections] +
            [f'n{collection}' for collection in skip_collections] +
            skip_branches)
        self._filter_branches = re.compile(f'^(?!({"|".join(skipped)})).*$')
        self._transform = NanoAOD(regular=False, jagged=True)

    def _filter(self, branches: set[str]):
        return {*filter(self._filter_branches.match, branches)}

    @abstractmethod
    def select(self, events):
        pass

    def process(self, events):
        selected = self.select(events)
        chunk = Chunk.from_coffea_events(events)
        dataset = events.metadata['dataset']
        result = {dataset: {
            'total_events': len(events),
            'saved_events': ak.sum(selected),
        }}
        filename = f'{dataset}/{_PICOAOD}_{chunk.uuid}_{chunk.entry_start}_{chunk.entry_stop}{_ROOT}'
        path = self._base / filename
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
    base_path: PathLike,
    output: dict[str, dict[str, list[Chunk]]],
    step: int,
    chunk_size: int
):
    base = EOS(base_path)
    transform = NanoAOD(regular=False, jagged=True)
    for dataset, chunks in output.items():
        output[dataset]['files'] = merge.resize(
            base / dataset/f'{_PICOAOD}{_ROOT}',
            *chunks['files'],
            step=step,
            chunk_size=chunk_size,
            reader_options={'transform': transform},
            dask=True,
        )
    return output
