# TODO
"""
High-level tools for ROOT file I/O built on top of :mod:`uproot`.

.. note::
    :mod:`pandas` will not be imported unless necessary.
"""
from .tree import Tree


__all__ = ['Tree']
