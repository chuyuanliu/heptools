'''
|               |                                   |
| -             | -                                 |
| monitor       | https://landscape.fnal.gov/lpc/   |
'''

from __future__ import annotations

import getpass
from pathlib import Path

from .htcondor import LocalFile, Tarball, TransferInput


class LPC:
    _user = getpass.getuser()
    _scratch  = Path(f'/uscmst1b_scratch/lpc1/3DayLifetime/{_user}')
    _nobackup = Path(f'/uscms/home/{_user}/nobackup').resolve()

    @classmethod
    def nobackup(cls, path: str):
        return str(cls._nobackup / path)

    @classmethod
    def use_default_dir(cls):
        TransferInput.set_scratch(cls._scratch)
        Tarball.set_base(cls.nobackup('.condor_tarball'))
        LocalFile.mount(cls._scratch, cls._nobackup)