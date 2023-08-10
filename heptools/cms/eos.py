from __future__ import annotations

from pathlib import Path
from subprocess import PIPE, CalledProcessError, check_output

__all__ = ['EOS']

# TODO wildcard, remove

class EOS:
    def __init__(self, url: str, path: str | Path):
        self.url = url
        path = str(path)
        if not path.startswith('/'):
            path = '/' + path
        self.path = Path(path)

    def join(self, *paths):
        return EOS(self.url, self.path.joinpath(*paths))

    def call(self, command: str, *args):
        return check_output(['eos', self.url, command, *args, str(self.path)], stderr = PIPE)

    def ls(self, arg: str = ''):
        arg = ['ls'] + ([arg] if arg else [])
        return self.call(*arg).decode().split('\n')

    def mkdir(self):
        if not self.exists():
            self.parent().mkdir()
            self.call('mkdir')

    def parent(self):
        return EOS(self.url, self.path.parent)

    def exists(self):
        try:
            self.ls()
            return True
        except CalledProcessError:
            return False

    def __str__(self):
        return f'{self.url}/{self.path}'