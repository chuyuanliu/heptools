from __future__ import annotations

import getpass
from pathlib import Path

from ...dataset import File
from ..eos import EOS
from .htcondor import HTCondor, LocalFile, Tarball, TransferInput
from .sites import CMSSites

__all__ = ['LPC']

class LPC:
    '''
    |               |                                   |
    | -             | -                                 |
    | monitor       | https://landscape.fnal.gov/lpc/   |
    '''
    priority = CMSSites('T3_US_FNALLPC', 'T1_US_FNAL_Disk')

    eos = EOS(f'/store', CMSSites.T3_US_FNALLPC)
    scratch = EOS('/uscmst1b_scratch/lpc1/3DayLifetime')

    user = getpass.getuser()
    nobackup = EOS(Path(f'/uscms/home/{user}/nobackup').resolve())

    @classmethod
    def setup_condor(cls):
        HTCondor.open_ports = (10000, 10200)
        TransferInput.set_scratch(cls.scratch / cls.user)
        Tarball.set_base(cls.nobackup / '.condor_tarball')
        LocalFile.mount(cls.scratch, cls.nobackup)
        File.priority = cls.priority