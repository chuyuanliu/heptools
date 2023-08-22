from __future__ import annotations

import tarfile
import uuid
from abc import ABC, abstractmethod, abstractproperty
from typing import Literal

import numpy as np

from ...utils import match_any
from ..eos import EOS, PathLike, load, save

__all__ = ['TransferInput', 'Tarball', 'LocalFile',
           'HTCondor']

class TransferInput(ABC):
    _scratch: EOS = None

    @staticmethod
    def set_scratch(path: PathLike):
        TransferInput._scratch = EOS(path)
        TransferInput._scratch.mkdir(recursive = True)

    @abstractproperty
    def inputs(self) -> list[str]:
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
    _cache_enabled: bool = True

    _base : EOS = None

    @property
    def inputs(self):
        return [str(self.tarball)]

    @property
    def prologue(self):
        return [f'tar -xzf {self.tarball.name}']

    def clean(self):
        if not self._cache_enabled:
            self.tarball.rm()

    @classmethod
    def set_base(cls, path: PathLike = ..., enable_cache: bool = True, metadata: str = '.cache'):
        cls._base = cls._scratch if path is ... else EOS(path)
        cls._base.mkdir(recursive = True)
        cls._cache_enabled = enable_cache
        if cls._cache_enabled:
            cls._cache_path = cls._base.join(metadata)
            if cls._cache_path.exists:
                cls._cache: dict[Tarball, EOS] = load(cls._cache_path)
                for cache in (*cls._cache,):
                    if not cache.is_valid:
                        cache.tarball.rm()
                        del cls._cache[cache]
                save(cls._cache_path, cls._cache)
        else:
            cls._cache = {}
            cls._cache_path = None

    @classmethod
    def reset_cache(cls):
        if cls._cache_enabled:
            for cache in cls._cache.values():
                cache.rm()
            cls._cache = {}
            cls._cache_path.rm()

    def __init__(self, *inputs: str | tuple[str, str], algorithm: Literal['gz', 'bz2', 'xz'] = 'gz', compresslevel: int = 4):
        if self._base is None:
            raise NotADirectoryError(f'call {self.set_base.__qualname__} before creating {Tarball.__qualname__}')
        files: list[tuple[str, str, int]] = []
        for src in inputs:
            if isinstance(src, tuple):
                src, dst = EOS(src[0]), EOS(src[1])
            elif isinstance(src, str):
                src = EOS(src)
                dst = EOS(src.name)
            for path, stat in src.scan():
                mtime = stat.st_mtime_ns
                files.append((str(path), str(dst.join(path.relative_to(src))), mtime))
        if len(files) == 0:
            raise FileNotFoundError(f'no file found in {inputs}')
        self.files = frozenset(files)
        self.tarball = None
        if self._cache_enabled:
            self.tarball = self._cache.get(self)
        if self.tarball is None:
            self.tarball = self._base.join(f'{uuid.uuid4()}.tar.{algorithm}')
            with tarfile.open(str(self.tarball), f'w:{algorithm}', compresslevel = compresslevel) as tar:
                for src, dst, _ in self.files:
                    tar.add(src, arcname = dst)
            if self._cache_enabled:
                self._cache[self] = self.tarball
                save(self._cache_path, self._cache)

    @property
    def is_valid(self):
        if not self.tarball.exists:
            return False
        for file, _, stamp in self.files:
            file = EOS(file)
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
        return [str(file) for file in self.files]

    @property
    def prologue(self):
        return []

    def clean(self):
        for file in self.copied:
            file.rm(recursive = True)

    @classmethod
    def mount(cls, *paths: PathLike):
        cls._mount.extend(EOS(path) for path in paths)

    def __init__(self, *inputs: str):
        self.files : list[EOS] = []
        self.copied: list[EOS] = []
        for src in inputs:
            src = EOS(src)
            if not match_any(src, self._mount, lambda x, y: x.isin(y)):
                src.copy_to(self._scratch, recursive = True)
                src = self._scratch.join(src.name)
                self.copied.append(src)
            self.files.extend(src.walk())

# TODO create Condor Cluster
class HTCondor(ABC):
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

    @abstractmethod
    def config(cls, *args, **kwargs) -> dict[str]:
        ...

    def __init__(self):
        ... # TODO
