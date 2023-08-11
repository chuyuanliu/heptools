from __future__ import annotations

from datetime import datetime
from pathlib import Path
from subprocess import PIPE, CalledProcessError, check_output
from typing import Union

__all__ = ['EOS', 'PathLike']

# TODO wildcard

class EOS:
    history: list[tuple[str, str]] = []

    def __init__(self, path: PathLike, url: str = None):
        if isinstance(path, EOS):
            default, path = path.url, path.path
        else:
            default = ''
        self.url  = default if url is None else url
        self.path = Path(path)

    @property
    def is_local(self):
        return not self.url

    @classmethod
    def cmd(cls, *args):
        args = [str(arg) for arg in args if arg]
        cls.history.append((datetime.now(), ' '.join(args)))
        return check_output([*args], stderr = PIPE)

    def call(self, command: str, *args):
        eos = () if self.is_local else ('eos', self.url)
        return self.cmd(*eos, command, *args)          

    def ls(self, *args: str):
        return self.call('ls', *args, self.path).decode().split('\n')

    def rm(self, *args: str):
        if self.exists():
            self.call('rm', *args, self.path)

    def copy_to(self, dest: PathLike, parents: bool = False, override: bool = False, *args: str):
        self.cp(self, dest, parents, override, *args)

    def move_to(self, dest: PathLike, parents: bool = False, override: bool = False, *args: str):
        self.mv(self, dest, parents, override, *args)

    @classmethod
    def cp(cls, src: PathLike, dest: PathLike, parents: bool = False, override: bool = False, *args: str):
        src, dest = EOS(src), EOS(dest)
        if not src.exists():
            raise FileNotFoundError(f'{src} does not exist')
        if not override and dest.exists():
            raise FileExistsError(f'{dest} already exists')
        if parents:
            dest.parent.mkdir(parents = True, exist_ok = True)
        else:
            if not dest.parent.exists():
                raise FileNotFoundError(f'{dest.parent} does not exist')
        if src.is_local and dest.is_local:
            cls.cmd('cp', src.path, dest.path, *args)
        else:
            cls.cmd('xrdcp', src, dest, *args)

    @classmethod
    def mv(cls, src: PathLike, dest: PathLike, parents: bool = False, override: bool = False, *args: str):
        cls.cp(src, dest, parents, override, *args)
        if dest.exists():
            EOS(src).rm()

    # Path
    def joinpath(self, *other: str | Path):
        return EOS(self.path.joinpath(*other), self.url)

    def mkdir(self, parents = False, exist_ok = False):
        if not self.exists():
            if parents:
                self.parent.mkdir(parents = parents, exist_ok = exist_ok)
            self.call('mkdir', self.path)
        elif not exist_ok:
            raise FileExistsError(f'{self} already exists')

    @property
    def name(self):
        return self.path.name

    @property
    def stem(self):
        return self.path.stem

    @property
    def parent(self):
        return EOS(self.path.parent, self.url)

    def is_dir(self):
        return self.path.is_dir()

    def exists(self):
        try:
            self.ls()
            return True
        except CalledProcessError:
            return False

    def __str__(self):
        return f'{self.url}{self.path}'

PathLike = Union[str, Path, EOS]