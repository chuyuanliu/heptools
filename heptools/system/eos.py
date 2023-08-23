from __future__ import annotations

import importlib
import os
import pickle
import re
from datetime import datetime
from pathlib import Path
from subprocess import PIPE, CalledProcessError, check_output
from typing import Any, Generator, Literal

__all__ = ['EOS', 'PathLike']

# TODO wildcard

class EOS:
    _url_pattern   = re.compile(r'^[\w]+://[\w.-]+')
    _slash_pattern = re.compile(r'(?<!:)/{2,}')

    run: bool = True
    client: Literal['eos', 'xrdfs'] = 'xrdfs'

    history: list[tuple[datetime, str, tuple[bool, bytes]]] = []

    def __init__(self, path: PathLike, url: str = ...):
        default = ''
        if isinstance(path, EOS):
            default, path = path.url, path.path
        elif isinstance(path, str):
            match = self._url_pattern.match(path)
            if match:
                default = match.group(0)
                path = path[len(default):]
        self.url = default if url is ... else url
        if self.url and not self.url.endswith('/'):
            self.url += '/'
        self.path = Path(self._slash_pattern.sub('/', str(path)))

    @property
    def is_local(self):
        return not self.url

    @property
    def is_dir(self):
        if not self.is_local:
            raise NotImplementedError(f'"{EOS.is_dir.fget.__qualname__}" only works for local files') # TODO
        return self.path.is_dir()

    @property
    def is_file(self):
        if not self.is_local:
            raise NotImplementedError(f'"{EOS.is_file.fget.__qualname__}" only works for local files') # TODO
        return not self.is_dir

    @property
    def exists(self):
        if not self.is_local:
            raise NotImplementedError(f'"{EOS.exists.fget.__qualname__}" only works for local files') # TODO
        return self.path.exists()

    @classmethod
    def cmd(cls, *args) -> tuple[bool, bytes]:
        args = [str(arg) for arg in args if arg]
        if cls.run:
            try:
                output = (True, check_output(args, stderr = PIPE))
            except CalledProcessError as e:
                output = (False, e.stderr)
        else:
            output = (True, b'')
        cls.history.append((datetime.now(), ' '.join(args), output))
        return output

    def call(self, executable: str, *args):
        eos = () if self.is_local else (self.client, self.url)
        return self.cmd(*eos, executable, *args)

    def rm(self, recursive: bool = False):
        if not self.is_local and recursive and self.client == 'xrdfs':
            raise NotImplementedError(f'"{self.rm.__qualname__}" does not support recursive removal of remote files using "xrdfs" client') # TODO
        return self.call('rm', '-r' if recursive else '', self.path)[0]

    def mkdir(self, recursive: bool = False):
        if self.call('mkdir', '-p' if recursive else '', self.path)[0]:
            return self

    def join(self, *other: str):
        return EOS(self.path.joinpath(*other), self.url)

    def walk(self) -> Generator[EOS, Any, None]:
        if not self.is_local:
            raise NotImplementedError(f'"{self.walk.__qualname__}" only works for local files') # TODO
        if self.is_file:
            yield self
        else:
            for root, _, files in os.walk(self.path):
                root = EOS(root, self.url)
                for file in files:
                    yield root / file

    def scan(self) -> Generator[tuple[EOS, os.stat_result], Any, None]:
        if not self.is_local:
            raise NotImplementedError(f'"{self.scan.__qualname__}" only works for local files') # TODO
        if self.is_file:
            yield self, self.stat()
        else:
            for entry in os.scandir(self.path):
                if entry.is_dir():
                    yield from EOS(entry.path).scan()
                else:
                    yield EOS(entry.path, self.url), entry.stat()

    def stat(self):
        if not self.is_local:
            raise NotImplementedError(f'"{self.stat.__qualname__}" only works for local files') # TODO
        return self.path.stat()

    def isin(self, other: PathLike):
        return str(self).startswith(str(other))

    def relative_to(self, other: PathLike):
        if self.url == other.url:
            return str(self.path.relative_to(other.path))
        else:
            raise ValueError(f'"{self}" is not in the subpath of "{other}"')

    def copy_to(self, dest: PathLike, parents: bool = False, overwrite: bool = False, recursive: bool = False):
        return self.cp(self, dest, parents, overwrite, recursive)

    def move_to(self, dest: PathLike, parents: bool = False, overwrite: bool = False, recursive: bool = False):
        return self.mv(self, dest, parents, overwrite, recursive)

    @classmethod
    def cp(cls, src: PathLike, dest: PathLike, parents: bool = False, overwrite: bool = False, recursive: bool = False):
        src, dest = EOS(src), EOS(dest)
        if parents:
            dest.parent.mkdir(recursive = True)
        if src.is_local and dest.is_local:
            result = cls.cmd('cp',
                             '-r' if recursive else '',
                             '-n' if not overwrite else '',
                             src, dest)
        else:
            if recursive:
                raise NotImplementedError(f'"{cls.cp.__qualname__}" does not support recursive copying of remote files') # TODO
            result = cls.cmd('xrdcp',
                             '-f' if overwrite else '',
                             src, dest)
        if result[0]:
            return dest

    @classmethod
    def mv(cls, src: PathLike, dest: PathLike, parents: bool = False, overwrite: bool = False, recursive: bool = False):
        src, dest = EOS(src), EOS(dest)
        if parents:
            dest.parent.mkdir(recursive = True)
        if src.url == dest.url:
            result = src.call('mv',
                              '-n' if not overwrite and src.client != 'xrdfs' else '',
                              src.path, dest.path)
        else:
            if recursive:
                raise NotImplementedError(f'"{cls.mv.__qualname__}" does not support recursive moving of remote files from different sites') # TODO
            result = cls.cp(src, dest, parents, overwrite, recursive)
            if result[0]:
                result = src.rm()
        if result[0]:
            return dest

    @property
    def name(self):
        return self.path.name

    @property
    def stem(self):
        return self.path.stem

    @property
    def extension(self):
        return self.path.suffix.removeprefix('.')

    @property
    def parent(self):
        return EOS(self.path.parent, self.url)

    def __hash__(self):
        return hash((self.url, self.path))

    def __eq__(self, other):
        if isinstance(other, EOS):
            return self.url == other.url and self.path == other.path
        elif isinstance(other, str | Path):
            return self == EOS(other)
        return NotImplemented

    def __str__(self): # TODO rich, __repr__
        return self.url + str(self.path)

    def __fspath__(self):
        return str(self.path)

    def __truediv__(self, other: str):
        return self.join(other)

PathLike = str | Path | EOS

def open_zip(algorithm: Literal['', 'gzip', 'bz2', 'lzma'], file: PathLike, mode: str, **kwargs):
    if not algorithm:
        return open(file, mode, **kwargs)
    module = importlib.import_module(algorithm)
    default = {}
    if algorithm in ['gzip', 'bz2']:
        default['compresslevel'] = 4
    return module.open(file, mode, **(default | kwargs))

def save(file: PathLike, obj, algorithm: Literal['', 'gzip', 'bz2', 'lzma'] = 'gzip', **kwargs):
    file = EOS(file)
    if file.is_local:
        pickle.dump(obj, open_zip(algorithm, file, 'wb', **kwargs))
    else:
        raise NotImplementedError(f'"{save.__qualname__}" does not support remote files') # TODO

def load(file: PathLike, algorithm: Literal['', 'gzip', 'bz2', 'lzma'] = 'gzip', **kwargs):
    file = EOS(file)
    if file.is_local:
        return pickle.load(open_zip(algorithm, file, 'rb', **kwargs))
    else:
        raise NotImplementedError(f'"{load.__qualname__}" does not support remote files') # TODO