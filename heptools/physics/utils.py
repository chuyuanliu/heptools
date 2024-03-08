from particle import latex_to_html_name
from particle.converters.bimap import DirectionalMaps

__all__ = ["PDGID"]


class PDGID:
    record, _ = DirectionalMaps("PDGID", "LATEXNAME", converters=(int, str))

    @classmethod
    def latex(cls, pdgid: int) -> str:
        return cls.record[pdgid]

    @classmethod
    def html(cls, pdgid: int) -> str:
        return latex_to_html_name(cls.latex(pdgid))
