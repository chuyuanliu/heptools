__all__ = ['CMSAAA', 'CERNBox']

class CMSAAA:
    GLOBAL  = 'root://cms-xrd-global.cern.ch/'
    US      = 'root://cmsxrootd.fnal.gov/'
    '''
        - T1_US_FNAL_Disk
    '''
    EU      = 'root://xrootd-cms.infn.it/'
    EOS_LPC = 'root://cmseos.fnal.gov/'
    '''
        - T3_US_FNALLPC
    '''

class CERNBox:
    '''
    |       |                               |
    | -     | -                             |
    | web   | https://cernbox.cern.ch/      |
    | docs  | https://cernbox.docs.cern.ch  |
    '''
    EOS_LXPLUS = 'root://eosuser.cern.ch/'