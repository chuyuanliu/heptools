from __future__ import annotations

from datetime import datetime
from pathlib import Path
from subprocess import PIPE, CalledProcessError, check_output
from typing import Union

__all__ = ['EOS', 'PathLike']

# TODO wildcard

class EOS:
    run_cmd: bool = True
    history: list[tuple[datetime, str, bytes]] = []

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

    @property
    def is_dir(self):
        # TODO
        ...

    @classmethod
    def cmd(cls, *args):
        args = [str(arg) for arg in args if arg]
        if cls.run_cmd:
            output = check_output([*args], stderr = PIPE)
        else:
            output = b''
        cls.history.append((datetime.now(), ' '.join(args), output))
        return output

    def call(self, command: str, *args):
        eos = () if self.is_local else ('eos', self.url)
        return self.cmd(*eos, command, *args)

    def ls(self, *args: str): # TODO return list of EOS
        return self.call('ls', *args, self.path).decode().split('\n')

    def rm(self, recursive: bool = False):
        self.call('rm', '-r' if recursive else '', self.path)

    def mkdir(self, recursive: bool = False):
        self.call('mkdir', '-p' if recursive else '', self.path)

    def join(self, *other: str):
        return EOS(self.path.joinpath(*other), self.url)

    def copy_to(self, dest: PathLike, parents: bool = False, override: bool = False, recursive: bool = False):
        self.cp(self, dest, parents, override, recursive)

    def move_to(self, dest: PathLike, parents: bool = False, override: bool = False, recursive: bool = False):
        self.mv(self, dest, parents, override, recursive)

    @classmethod
    def _dual_check(cls, src: PathLike, dest: PathLike, parents: bool, override: bool):
        src, dest = EOS(src), EOS(dest)
        if not src.exists:
            raise FileNotFoundError(f'"{src}" does not exist')
        if not override and dest.exists:
            raise FileExistsError(f'"{dest}" already exists')
        if parents:
            dest.parent.mkdir(recursive = True)
        else:
            if not dest.parent.exists:
                raise FileNotFoundError(f'"{dest.parent}" does not exist')
        return src, dest

    @classmethod
    def cp(cls, src: PathLike, dest: PathLike, parents: bool = False, override: bool = False, recursive: bool = False):
        src, dest = cls._dual_check(src, dest, parents, override)
        if src.is_local and dest.is_local:
            cls.cmd('cp', '-r' if recursive else '', src, dest)
        else:
            if recursive:
                raise NotImplementedError # TODO
            cls.cmd('xrdcp', src, dest)

    @classmethod
    def mv(cls, src: PathLike, dest: PathLike, parents: bool = False, override: bool = False, recursive: bool = False):
        src, dest = cls._dual_check(src, dest, parents, override)
        if src.url == dest.url:
            src.call('mv', src.path, dest.path)
        else:
            if recursive:
                raise NotImplementedError # TODO
            cls.cp(src, dest)
            src.rm()

    @property
    def name(self):
        return self.path.name

    @property
    def stem(self):
        return self.path.stem

    @property
    def parent(self):
        return EOS(self.path.parent, self.url)

    @property
    def exists(self):
        try:
            self.ls()
            return True
        except CalledProcessError:
            return False

    def __str__(self): # TODO rich
        return f'{self.url}{self.path}'

PathLike = Union[str, Path, EOS]