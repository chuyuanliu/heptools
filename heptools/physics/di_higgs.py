"""
Kappa framework for di-Higgs production processes [1]_.
[1] [arXiv:1212.5581](https://arxiv.org/abs/1212.5581)
"""

from .kappa_framework import Coupling, Diagram

__all__ = ["ggF", "VBF", "VHH", "ggZHH", "Coupling"]


class ggF(Diagram):
    diagrams = (
        ("kl", "kt"),
        (
            (1, 1),
            (0, 2),
        ),
    )


class VBF(Diagram):
    diagrams = (
        ("kl", "kv", "kvv"),
        (
            (1, 1, 0),
            (0, 2, 0),
            (0, 0, 1),
        ),
    )


class VHH(VBF): ...


class ggZHH(Diagram):
    diagrams = (
        ("kl", "kv", "kvv", "kt"),
        (
            (1, 1, 0, 0),
            (0, 2, 0, 0),
            (0, 0, 1, 0),
            (0, 0, 0, 2),
            (0, 1, 0, 1),
            (1, 0, 0, 1),
        ),
    )
