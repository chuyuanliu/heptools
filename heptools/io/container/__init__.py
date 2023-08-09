from .partial_set import PartialSet
from .tree import Tree

__all__ = ['PartialSet', 'Tree']

class ContainerError(Exception):
    __module__ = Exception.__module__