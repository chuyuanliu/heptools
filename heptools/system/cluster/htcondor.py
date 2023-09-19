from __future__ import annotations

import getpass
import socket
import tarfile
import uuid
from abc import ABC, abstractmethod, abstractproperty
from collections import defaultdict
from datetime import datetime
from typing import Iterable, Literal

import numpy as np
from dask_jobqueue.htcondor import HTCondorCluster, HTCondorJob
from rich.text import Text

from heptools.container import Tree

from ...container import Tree
from ...utils import match_any, unpack
from ..cvmfs import unpacked_cern_ch
from ..eos import EOS, PathLike, load, save

__all__ = ['TransferInput', 'Tarball', 'LocalFile',
           'HTCondor']

class TransferInput(ABC):
    _scratch: EOS = None

    @staticmethod
    def set_scratch(path: PathLike):
        TransferInput._scratch = EOS(path).mkdir(recursive = True)

    @abstractproperty
    def inputs(self) -> list[str]:
        ...

    @abstractproperty
    def tree(self) -> Tree[list[str]]:
        ...

    @abstractproperty
    def prologue(self) -> list[str]:
        ...

    @abstractmethod
    def clean(self):
        ...

class Tarball(TransferInput):
    _cache: dict[Tarball, EOS] = {}
    _cache_path: EOS = None

    _base : EOS = None

    @property
    def inputs(self):
        return [str(self.tarball)]

    @property
    def tree(self):
        unpacked = Tree(list[str])
        for src, dst, _ in self.files:
            unpacked[unpack((*str(dst).split('/'), ))].append((str(src), self.tarball.name))
        return unpacked

    @property
    def prologue(self):
        return [f'tar -xzf {self.tarball.name}', f'rm {self.tarball.name}']

    def clean(self):
        if not self.cached:
            self.tarball.rm()

    @classmethod
    def set_base(cls, path: PathLike = ..., enable_cache: bool = True, metadata: str = '.cache'):
        cls._base = (cls._scratch if path is ... else EOS(path)).mkdir(recursive = True)
        cls._cache = {}
        cls._cache_path = None
        if enable_cache and cls._base is not None:
            cls._cache_path = cls._base / metadata
            if cls._cache_path.exists:
                cls._cache: dict[Tarball, EOS] = load(cls._cache_path)
                for cache in (*cls._cache,):
                    if not cache.tarball.exists:
                        cls._cache.pop(cache)
                save(cls._cache_path, cls._cache)

    @classmethod
    def reset_cache(cls):
        if cls._cache_path is not None:
            for cache in cls._cache.values():
                cache.rm()
            cls._cache = {}
            cls._cache_path.rm()

    @classmethod
    def remove_invalid_cache(cls):
        if cls._cache_path is not None:
            for cache in (*cls._cache,):
                if not cache.is_valid:
                    cls._cache.pop(cache).rm()
            save(cls._cache_path, cls._cache)

    def __init__(self, *inputs: PathLike | tuple[PathLike, PathLike], algorithm: Literal['gz', 'bz2', 'xz'] = 'gz', compresslevel: int = 4):
        if self._base is None:
            raise NotADirectoryError(f'call `{self.set_base.__qualname__}()` before creating <{Tarball.__qualname__}>')
        files: list[tuple[EOS, EOS, int]] = []
        for src in inputs:
            if isinstance(src, tuple):
                src, dst = EOS(src[0]), EOS(src[1])
            elif isinstance(src, PathLike):
                src = EOS(src)
                dst = EOS(src.name)
            else:
                raise TypeError(f'invalid type <{type(src)}>')
            if not (src.is_local and dst.is_local):
                raise ValueError(f'cannot use remote path "{src}" -> "{dst}"')
            for path, stat in src.scan():
                mtime = stat.st_mtime_ns
                files.append((path, dst / path.relative_to(src), mtime))
        if len(files) == 0:
            raise FileNotFoundError(f'no file found in {inputs}')
        self.files = frozenset(files)
        self.tarball = self._cache.get(self)
        self.cached = self._cache_path is not None
        if self.tarball is None:
            self.tarball = self._base / f'{uuid.uuid4()}.tar.{algorithm}'
            with tarfile.open(self.tarball, f'w:{algorithm}', compresslevel = compresslevel) as tar:
                for src, dst, _ in self.files:
                    tar.add(src, arcname = dst)
            if self.cached:
                self._cache[self] = self.tarball
                save(self._cache_path, self._cache)

    @property
    def is_valid(self):
        if not self.tarball.exists:
            return False
        for file, _, stamp in self.files:
            try:
                if file.stat().st_mtime_ns != stamp:
                    return False
            except:
                return False
        return True

    def __hash__(self):
        return hash(self.files)

    def __eq__(self, other: Tarball):
        if isinstance(other, Tarball):
            return self.files == other.files
        return NotImplemented

class LocalFile(TransferInput):
    _mount: list[EOS] = []

    @property
    def inputs(self):
        return [str(file) for file in self.files + [*self.copied]]

    @property
    def tree(self) -> Tree[list[str]]:
        files = Tree(list[str])
        for file in self.files:
            files[file.name].append(str(file))
        for dst, srcs in self.copied.items():
            for src in srcs:
                files[dst.name].append((str(src), str(dst)))
        return files

    @property
    def prologue(self):
        return []

    def clean(self):
        for file in self.copied:
            file.rm()

    @classmethod
    def mount(cls, *paths: PathLike):
        cls._mount.extend(EOS(path) for path in paths)

    def __init__(self, *inputs: PathLike):
        self.files : list[EOS] = []
        self.copied = defaultdict[EOS, list[EOS]](list)
        for src in inputs:
            src = EOS(src)
            if not match_any(src, self._mount, lambda x, y: x.isin(y)):
                for file in src.walk():
                    dst = self._scratch / file.name
                    file.copy_to(dst)
                    self.copied[dst].append(file)
            else:
                self.files.extend(src.walk())

class HTCondor:
    open_ports: tuple[int, int] = (0, 65535)

    @classmethod
    def random_port(cls, *ports: int) -> tuple[int, ...]:
        ports = np.asarray(ports)
        used = np.unique(ports[~np.isin(ports, [..., None])])
        required = (ports == ...)
        ports[required] = np.random.choice(
            np.setdiff1d(np.arange(cls.open_ports[0], cls.open_ports[1] + 1), used),
            np.sum(required))
        return (*ports,)

    def __init__(self,
                 name: str = 'dask',
                 cores: int = 1,
                 memory: int | str = ...,
                 disk: int | str = '40GB',
                 image: str = 'chuyuanliu/heptools:latest',
                 log: PathLike = ...,
                 scheduler_port: int = ...,
                 dashboard_port: int = ...,
                 inputs: Iterable[PathLike | tuple[PathLike, PathLike] | TransferInput] = None,
                 **kwargs):
        ''' optional args
            - `job_extra_directives: dict[str]`
            - `job_script_prologue: list[str]`
            - `scheduler_options: dict[str]`
            - `worker_extra_args: list[str]`
        '''
        if memory is ...:
            memory = f'{cores * 2}GB'

        if log is ...:
            log = TransferInput._scratch / 'condor_logs'
        if log is not None:
            log = str(EOS(log).join(datetime.now().strftime(f'{name}__%Y_%m_%d__%H_%M_%S')).mkdir(recursive = True))
        self._log = log

        scheduler_port, self._dashboard_port = self.random_port(scheduler_port, dashboard_port)
        mid_port = sum(self.open_ports) // 2

        if inputs is None:
            inputs = ()
        self._inputs: list[TransferInput] = []
        tarball, files = [], []
        for file in inputs:
            if isinstance(file, TransferInput):
                self._inputs.append(file)
            elif isinstance(file, tuple):
                tarball.append(file)
            elif isinstance(file, PathLike):
                file = EOS(file)
                if file.is_dir:
                    tarball.append(file)
                else:
                    files.append(file)
        if len(tarball) > 0:
            self._inputs.append(Tarball(*tarball))
        if len(files) > 0:
            self._inputs.append(LocalFile(*files))

        self._excecutable = TransferInput._scratch / f'condor_exec.exe'
        with open(self._excecutable, 'w') as f:
            f.write('\n'.join(['#!/bin/bash', 'eval $2']))
        HTCondorJob.executable = str(self._excecutable)

        self._cluster = HTCondorCluster(
            cores = cores,
            memory = memory,
            disk = disk,
            local_directory = '/srv',
            log_directory = self._log,
            python = 'python',

            job_extra_directives = {
                'batch_name': name,
                'use_x509userproxy': True,
                'should_transfer_files': 'YES',
                'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
                'initialdir': str(self._log),
                'transfer_input_files': ','.join(set(sum([input.inputs for input in self._inputs], []))),
                '+SingularityImage': f'"{unpacked_cern_ch(image)}"',
            } | kwargs.pop('job_extra_directives', {}),

            job_script_prologue = sum([input.prologue for input in self._inputs], [])
              + kwargs.pop('job_script_prologue', []),

            scheduler_options={
                'dashboard_address': self._dashboard_port,
                'port': scheduler_port,
            } | kwargs.pop('scheduler_options', {}),

            worker_extra_args = [
                f'--worker-port {self.open_ports[0]}:{mid_port}',
                f'--nanny-port {mid_port}:{self.open_ports[1]}'
            ] + kwargs.pop('worker_extra_args', []),

            **kwargs
        )

    @property
    def cluster(self):
        return self._cluster

    def check_inputs(self): # TODO rich.print temp
        raw = sum([input.tree for input in self._inputs], Tree(list[str]))
        tree = Tree(Text)
        for path, files in raw.walk():
            args = {'style': 'default'}
            if len(files) > 1:
                args['style'] = 'red'
            text = Text(**args)
            for file in files:
                if isinstance(file, str):
                    text += Text('  ' + file + '\n', **args)
                elif isinstance(file, tuple):
                    text += Text(f'  {file[0]} -> {file[1]}\n', **args)
            tree[unpack(path)] += text
        return tree

    def dashboard(self, local_port = ...):
        if self._dashboard_port is None:
            return ''
        else:
            if local_port is ...:
                local_port = self._dashboard_port
            return '\n'.join([
                f'python -m webbrowser "http://localhost:{local_port}"',
                f'ssh -L {local_port}:localhost:{self._dashboard_port} {getpass.getuser()}@{socket.gethostname()}'
            ])

    def clean(self):
        self._excecutable.rm()
        for input in self._inputs:
            input.clean()

    @property
    def log(self):
        logs = {'log': [], 'err': [], 'out': []}
        for file in EOS(self._log).walk():
            ext = file.extension
            if ext in logs:
                logs[ext].append(open(file, 'r').read())
        return logs