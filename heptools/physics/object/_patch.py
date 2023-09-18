import awkward as ak


def patch_coffea_nanoevent():
    from coffea.nanoevents.methods.vector import LorentzVector, TwoVector
    TwoVector.st = property(
        lambda self: self.pt)
    TwoVector.ht = property(
        lambda self: self.st)
    LorentzVector.p4vec = property(
        lambda self: ak.zip({
            'x': self.x,
            'y': self.y,
            'z': self.z,
            't': self.t,},
            with_name = 'LorentzVector'))

    from ... import behavior
    for name in ['GenParticle',
                 'Electron', 'Muon', 'Tau', 'Photon', 'FsrPhoton',
                 'Jet', 'FatJet']:
        behavior[('__typestr__', name)] = name
