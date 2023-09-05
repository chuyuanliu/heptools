__all__ = ['Sites', 'CMSSites']

class Sites:
    GLOBAL = None

    def __init__(self, *priority: str):
        if self.GLOBAL is None:
            raise RuntimeError(f'default site "{self.__class__.__name__}.GLOBAL" is not defined')
        self.priority = priority

    def find(self, sites: list[str]) -> str:
        if not sites:
            return ''
        for priority in self.priority:
            url = getattr(self, priority)
            if priority in sites or url in sites:
                return url
        return self.GLOBAL

class CMSSites(Sites):
    GLOBAL              = 'root://cms-xrd-global.cern.ch/'
    T1_US_FNAL_Disk     = 'root://cmsxrootd.fnal.gov/'
    T3_US_FNALLPC       = 'root://cmseos.fnal.gov/'