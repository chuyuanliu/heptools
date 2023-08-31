from coffea.nanoevents import NanoAODSchema
from coffea.processor import ProcessorABC, Runner, iterative_executor
from dask import compute, delayed

from heptools.root.dataset import Dataset, File, FileList
from heptools.root.skim import PicoAOD
from heptools.system.eos import EOS, PathLike
from heptools.utils import ensure


@delayed
def skim(processor: ProcessorABC, output: EOS, inputs: dict, lazy_chunksize: int, full_chunksize: int):
    runner = Runner(
        executor = iterative_executor(),
        schema = NanoAODSchema,
        chunksize = lazy_chunksize,
    )
    selection = runner(inputs, 'Events', processor)
    chunkAOD = PicoAOD.copy()
    chunkAOD.unique_index = None
    chunkAOD.iterate_step = full_chunksize
    def select(x):
        return x[selection(x['run'], x['luminosityBlock'], x['event'])]
    chunkAOD(output, sum((v['files'] for v in inputs.values()), []), select)
    return output

@delayed
def merge(output: EOS, inputs: list[EOS], full_chunksize: int):
    mergeAOD = PicoAOD.copy()
    mergeAOD.iterate_step = full_chunksize
    count = mergeAOD(output, inputs)
    for chunk in inputs:
        chunk.rm()
    return output, count

def create_picoaod_from_dataset(
        base: PathLike,
        filelists: Dataset,
        lazy_chunksize: int = 500_000,
        full_chunksize: int = 100_000, # TODO adaptive based on memory
        **selections: ProcessorABC):
    base = EOS(base)
    outputs = []
    for (source, dataset, year, era, _), filelist in filelists:
        path = (base / source / dataset / (year + era)).mkdir(recursive = True)
        metadata = {
            'source': source,
            'year': year,
            'era': era,
        }
        files: list[tuple[EOS, str]] = []
        for file in filelist.files:
            if not file.site: # TODO automatically choose site based on download speed. rucio?
                site = ''
            elif 'T1_US_FNAL_Disk' in file.site:
                site = 'root://cmsxrootd.fnal.gov/'
            else:
                site = 'root://cms-xrd-global.cern.ch/'
            files.append((
                EOS(file.path, site),
                f'picoAOD{{selection}}.chunk{len(files)}.root'
            ))
        for selection, processor in selections.items():
            if selection:
                selection = ensure(selection, '_')
            chunks = []
            for i, o in files:
                chunks.append(
                    skim(
                        processor = processor,
                        output = path / o.format(selection = selection),
                        inputs = {dataset: {'files': [str(i)], 'metadata': metadata}},
                        lazy_chunksize = lazy_chunksize,
                        full_chunksize = full_chunksize,
                    )
                )
            outputs.append(
                (merge(
                    output = path / f'picoAOD{selection}.root',
                    inputs = chunks,
                    full_chunksize = full_chunksize,
                ), source, dataset, year, era, selection)
            )

    outputs = compute(*outputs)
    skimmed = Dataset()
    for ((output, count), source, dataset, year, era, selection) in outputs:
        skimmed.update(source, dataset, year, era, f'PicoAOD{selection}',
                       FileList({
                           'path'   : str(output.path.parent),
                           'nevents': count,
                           'nfiles' : 1,
                           'files'  : [File({
                               'site'   : [output.url],
                               'path'   : str(output.path),
                               'nevents': count})],}))
    return skimmed