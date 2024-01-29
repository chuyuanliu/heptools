import re
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor

import awkward as ak
import uproot
from coffea.processor import ProcessorABC, accumulate

from heptools.awkward.zip import NanoAOD
from heptools.root.chunk import Chunk
from heptools.root.io import TreeReader, TreeWriter
from heptools.system.eos import EOS, PathLike

_PICOAOD = 'picoAOD'


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
        # category = events.metadata['category'] # TODO mc, dataset, year
        category = 'test'  # TODO remove
        result = {category: {
            'nevents': len(events),
        }}
        filename = f'{category}/{_PICOAOD}_{chunk.uuid}_{chunk.entry_start}_{chunk.entry_stop}.root'
        path = self._basepath / filename
        with TreeWriter()(path) as writer:
            for i, data in enumerate(TreeReader(self._filter, self._transform).iterate(self._step, chunk)):
                writer.extend(data[selected[i*self._step:(i+1)*self._step]])
        result[category]['files'] = [writer.tree]

        return result

    def postprocess(self, accumulator):
        pass


def _fetch_metadata(category: str, path: PathLike):
    with uproot.open(path) as f:
        data = f['Runs'].arrays(
            ['genEventCount', 'genEventSumw', 'genEventSumw2'])
        return {
            category: {
                'count': ak.sum(data['genEventCount']),
                'sumw': ak.sum(data['genEventSumw']),
                'sumw2': ak.sum(data['genEventSumw2']),
            }
        }


def fetch_metadata(**paths: list[PathLike]) -> dict[str, dict[str]]:
    count = sum(len(path) for path in paths.values())
    with ThreadPoolExecutor(max_workers=count) as executor:
        tasks = []
        for category, path in paths.items():
            for p in path:
                tasks.append(executor.submit(_fetch_metadata, category, p))
        results = [task.result() for task in tasks]
    return accumulate(results)
