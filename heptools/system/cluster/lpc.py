from __future__ import annotations

import getpass
from pathlib import Path

from ..eos import EOS
from ..xrootd import CMSAAA
from .htcondor import HTCondor, LocalFile, Tarball, TransferInput

__all__ = ["LPC"]


class LPC:
    """
    |               |                                   |
    | -             | -                                 |
    | monitor       | https://landscape.fnal.gov/lpc/   |
    """

    eos = EOS(f"/store", CMSAAA.EOS_LPC)
    scratch = EOS("/uscmst1b_scratch/lpc1/3DayLifetime")

    user = getpass.getuser()
    nobackup = EOS(Path(f"/uscms/home/{user}/nobackup").resolve())

    @classmethod
    def setup_condor(cls):
        HTCondor.open_ports = (10000, 10200)
        TransferInput.set_scratch(cls.scratch / cls.user)
        Tarball.set_base(cls.nobackup / ".condor_tarball")
        LocalFile.mount(cls.scratch, cls.nobackup)
