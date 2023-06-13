_json_POGs = {}
_json_POGs.update(dict.fromkeys(['btagging', 'ctagging', 'subjet_btagging'], 'BTV'))
_json_POGs.update(dict.fromkeys(['electron', 'photon'], 'EGM'))
_json_POGs.update(dict.fromkeys(['fatJet_jerc', 'jet_jerc', 'jetvetomaps', 'jmar', 'met'], 'JME'))
_json_POGs.update(dict.fromkeys(['puWeights'], 'LUM'))
_json_POGs.update(dict.fromkeys(['muon_Z'], 'MUO'))
_json_POGs.update(dict.fromkeys(['tau'], 'TAU'))

def json_POG_integration(era: str, file: str):
    '''
        - `era` = `2016preVFP_UL`, `2016postVFP_UL`, `2017_UL`, `2018_UL`
        - https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration/-/tree/master/
    '''
    return f'/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/{_json_POGs[file]}/{era}/{file}.json.gz'