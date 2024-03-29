__all__ = ["jsonPOG_integration", "unpacked_cern_ch"]

_jsonPOG_integration_groups = (
    dict.fromkeys(["btagging", "ctagging", "subjet_btagging"], "BTV")
    | dict.fromkeys(["electron", "photon"], "EGM")
    | dict.fromkeys(["fatJet_jerc", "jet_jerc", "jetvetomaps", "jmar", "met"], "JME")
    | dict.fromkeys(["puWeights"], "LUM")
    | dict.fromkeys(["muon_Z"], "MUO")
    | dict.fromkeys(["tau"], "TAU")
)


def jsonPOG_integration(era: str, file: str):
    """
    - `era` = `2016preVFP_UL`, `2016postVFP_UL`, `2017_UL`, `2018_UL`
    - https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/tree/master/
    """
    return f"/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/{_jsonPOG_integration_groups[file]}/{era}/{file}.json.gz"


def unpacked_cern_ch(image: str):
    """
    - https://cvmfs.readthedocs.io/en/stable/cpt-containers.html#using-unpacked-cern-ch
    """
    return f"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/{image}"
