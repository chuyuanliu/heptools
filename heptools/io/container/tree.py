from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any, Callable, Generator, Generic, TypeVar

from ..._utils import match_any, unpack

_LeafType = TypeVar('_LeafType')
class Tree(defaultdict[str], Generic[_LeafType]):
    def __init__(self):
        super().__init__(Tree[_LeafType])

    def __getitem__(self, __key) -> Tree[_LeafType] | _LeafType:
        if isinstance(__key, tuple):
            return self[__key[0]][unpack(__key[1:])]
        return super().__getitem__(__key)

    def __setitem__(self, __key, __value) -> Tree[_LeafType] | _LeafType:
        if isinstance(__key, tuple):
            self[__key[0]][unpack(__key[1:])] = __value
        else:
            super().__setitem__(__key, __value)

    def walk(self, *pattern: list[str | list[str]]) -> Generator[tuple[tuple[str, ...], _LeafType]]:
        for k in self.keys():
            if not pattern or match_any(k, pattern[0], lambda x, y: re.match(y, x) is not None):
                if isinstance(self[k], Tree):
                    for meta, entry in self[k].walk(*(pattern[1:] if len(pattern) > 1 else [])):
                        yield (k, *meta), entry
                else:
                    yield (k,), self[k]

    def __str__(self):
        lines = []
        branches = sorted(self.keys())
        for i, k in enumerate(branches):
            if i == len(branches) - 1:
                lines.append((f' └─{k}\n' + str(self[k])).replace('\n', '\n  '))
            else:
                lines.append((f' ├─{k}\n' + str(self[k])).replace('\n', '\n │'))
        return '\n'.join(lines)

    def from_dict(self, tree: dict, depth: int = float('inf'), leaf: Callable[[Any], _LeafType] = None):
        for k, v in tree.items():
            if isinstance(v, dict) and depth > 1:
                self[k].from_dict(v, depth = depth - 1, leaf = leaf)
            else:
                self[k] = leaf(v) if leaf else v
        return self