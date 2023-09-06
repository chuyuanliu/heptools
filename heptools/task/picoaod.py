from coffea.nanoevents import NanoAODSchema
from coffea.processor import ProcessorABC, Runner, iterative_executor
from dask import compute, delayed

from heptools.benchmark.unit import Metric
from heptools.cms import PicoAOD
from heptools.dataset import Dataset, File, FileList
from heptools.root import Chunk
from heptools.system.eos import EOS, PathLike
from heptools.utils import ensure

FILENAME = 'picoAOD'

def _int(*values: int | str) -> tuple[int, ...]:
    return tuple(int(Metric.remove(v)) if isinstance(v, str) else v for v in values)

def _cluster_skim(output: PathLike, inputs: list[PathLike | Chunk], step: int, shift: int = None, selection = None):
    skim = PicoAOD.copy()
    skim.iterate_step = step
    if shift is None:
        skim.unique_index = None
        shift = 0

    local_output = EOS(output.name)
    nevents = skim(output = local_output,
                   inputs = inputs,
                   selection = selection,
                   index_shift = shift)
    local_output.move_to(output, parents = True, overwrite = True)

    return File(
        site = [output.url],
        path = str(output.path),
        nevents = nevents)

@delayed
def select(processor: ProcessorABC, output: EOS, inputs: dict, lazy_read_step: int, full_read_step: int):
    selection = Runner(
        executor = iterative_executor(),
        schema = NanoAODSchema,
        chunksize = lazy_read_step,
        xrootdtimeout = 3 * 60 * 60,
    )(inputs, 'Events', processor)
    return _cluster_skim(
        output = output,
        inputs = sum((v['files'] for v in inputs.values()), []),
        step = full_read_step,
        selection = lambda x: x[selection(x['run'], x['luminosityBlock'], x['event'])],
    )

@delayed
def merge(output: EOS, inputs: list[EOS], full_read_step: int, index_shift: int): # TODO
    return _cluster_skim(
        output = output,
        inputs = inputs,
        step = full_read_step,
        shift = index_shift,
    )

def create_picoaod_from_dataset(
        base: PathLike,
        datasets: Dataset,
        lazy_read_step: int | str = 500_000,
        full_read_step: int | str = 100_000,
        **selections: ProcessorABC):
    base = EOS(base)
    lazy_read_step, full_read_step = _int(lazy_read_step, full_read_step)
    outputs = []
    for (source, dataset, year, era, _), filelist in datasets:
        path = base / source / dataset / (year + era)
        metadata = {
            'source': source,
            'year': year,
            'era': era,
        }
        files: list[tuple[EOS, str]] = []
        for file in filelist:
            files.append((
                file.eos,
                f'{FILENAME}{{selection}}.chunk{len(files)}.root'
            ))
        for selection, processor in selections.items():
            if selection:
                selection = ensure(selection, '_')
            for i, o in files:
                outputs.append((
                    select(
                        processor = processor,
                        output = path / o.format(selection = selection),
                        inputs = {dataset: {'files': [str(i)], 'metadata': metadata}},
                        lazy_read_step = lazy_read_step,
                        full_read_step = full_read_step,
                    ), source, dataset, year, era, selection
                ))
    outputs = compute(*outputs)
    skimmed = Dataset()
    for (file, source, dataset, year, era, selection) in outputs:
        skimmed.update(source, dataset, year, era, f'{FILENAME}{selection}', FileList({'files': [file]}))
    return skimmed

def merge_chunks(
        base: PathLike,
        datasets: Dataset,
        chunksize: int | str = ...,
        full_read_step: int | str = 100_000,
        ):
    base = EOS(base)
    chunksize, full_read_step = _int(chunksize, full_read_step)
    outputs = []
    for (source, dataset, year, era, tier), filelist in datasets:
        shift = 0
        path = base / source / dataset / (year + era)
        files = [Chunk(f.eos, f.nevents) for f in filelist]
        if chunksize is ...:
            chunks = [files]
        else:
            chunks = [*Chunk.split(chunksize, *files)]
        for i, chunk in enumerate(chunks):
            input = chunk
            temp = path / f'{tier}.tmp{i}.root'
            output = path / f'{tier}.chunk{i}.root'
            outputs.append((
                merge(
                    output = temp,
                    inputs = input,
                    full_read_step = full_read_step,
                    index_shift = shift,
                ), temp, output, source, dataset, year, era, tier
            ))
            shift += sum(len(i) for i in chunk)
    
    outputs = compute(*outputs)
    for _, file in datasets.files:
        file.eos.rm()
    merged = Dataset()
    for (file, temp, output, source, dataset, year, era, tier) in outputs:
        temp.move_to(output, parents = True, overwrite = True)
        file = File(file, site = [output.url], path = str(output.path))
        merged.update(source, dataset, year, era, tier, FileList({'files': [file]}))
    return merged