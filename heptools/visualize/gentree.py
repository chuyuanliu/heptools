from os import PathLike

import awkward as ak
import graphviz

from ..physics.utils import PDGID

__all__ = ['genpart_graph']

def genpart_graph(event: ak.Array, output: PathLike | str, format: str = 'pdf'):
    particles = event.GenPart
    graph = graphviz.Digraph(format = format)
    for idx, particle in enumerate(particles):
        graph.node(str(idx), label = f'<{PDGID.html(particle.pdgId)}>')
        if particle.genPartIdxMother >= 0:
            graph.edge(str(particle.genPartIdxMother), str(idx))
    graph.render(output)