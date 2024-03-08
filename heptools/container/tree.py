from __future__ import annotations

import re
from copy import copy
from typing import Callable, Generator, Generic, TypeVar

from rich.text import Text

from ..utils import match_any, merge_op, unpack

_LeafT = TypeVar("_LeafT")


class Tree(dict[str], Generic[_LeafT]):
    def __init__(self, leaf: Callable[[], _LeafT] = None):
        self.leaf = leaf

    def __getitem__(self, __key) -> Tree[_LeafT] | _LeafT:
        if __key is None:
            return self
        if isinstance(__key, tuple):
            if len(__key) == 0:
                return self
            if __key[0] not in self:
                super().__setitem__(__key[0], Tree[_LeafT](self.leaf))
            return self[__key[0]][unpack(__key[1:])]
        elif isinstance(__key, str):
            if __key not in self:
                super().__setitem__(__key, self.leaf() if self.leaf else None)
            return super().__getitem__(__key)

    def __setitem__(self, __key, __value) -> Tree[_LeafT] | _LeafT:
        __key = unpack(__key)
        if isinstance(__key, tuple):
            self[(__key[0],)][__key[1:]] = __value
        elif isinstance(__key, str):
            super().__setitem__(__key, __value)

    def walk(
        self, *pattern: str | list[str]
    ) -> Generator[tuple[tuple[str, ...], _LeafT]]:
        for k in self.keys():
            if not pattern or match_any(k, pattern[0], re.match):
                if isinstance(self[k], Tree):
                    for meta, entry in self[k].walk(*pattern[1:]):
                        yield (k, *meta), entry
                else:
                    yield (k,), self[k]

    def __str__(self):  # TODO rich, __repr__
        lines = []
        keys = sorted(self.keys())
        for i, k in enumerate(keys):
            if i == len(keys) - 1:
                branch, joint = " ", "└─"
            else:
                branch, joint = "│", "├─"
            lines.append(
                (f" {joint}{k}\n" + str(self[k])).replace("\n", f"\n {branch}")
            )
        return "\n".join(lines)

    @property
    def rich(self):  # TODO rich, __repr__
        lines = []
        keys = sorted(self.keys())
        for i, k in enumerate(keys):
            s = self[k]
            if isinstance(s, Tree):
                s = self[k].rich
            elif not isinstance(s, Text):
                s = Text(str(s), style="default")
            line = [*s.split("\n")]
            if i == len(keys) - 1:
                branch, joint = " ", "└─"
            else:
                branch, joint = "│", "├─"
            lines.append(
                Text(f"\n {branch}", style="yellow").join(
                    [Text(f" {joint}", style="yellow") + Text(k, style="default")]
                    + line
                )
            )
        return Text("\n").join(lines)

    def iop(
        self, other: Tree[_LeafT], op: Callable[[_LeafT, _LeafT], _LeafT]
    ) -> Tree[_LeafT]:
        if isinstance(other, Tree):
            if not (self.leaf == other.leaf):
                raise TypeError(
                    f"cannot operate on trees with different leaf `{self.leaf}()` and `{other.leaf}()`"
                )
            for k, v in other.items():
                if isinstance(v, Tree):
                    self[(k,)].iop(v, op)
                else:
                    if k in self:
                        self[k] = merge_op(op, self[k], v)
                    else:
                        self[k] = copy(v)
            return self
        return NotImplemented

    @staticmethod
    def op(
        first: Tree[_LeafT],
        second: Tree[_LeafT],
        op: Callable[[_LeafT, _LeafT], _LeafT],
    ) -> Tree[_LeafT]:
        if isinstance(first, Tree) and isinstance(second, Tree):
            tree = Tree[_LeafT](first.leaf)
            tree.iop(first, op)
            tree.iop(second, op)
            return tree
        return NotImplemented

    def __ior__(self, other: Tree[_LeafT]) -> Tree[_LeafT]:
        return self.iop(other, lambda x, y: x | y)

    def __or__(self, other: Tree[_LeafT]) -> Tree[_LeafT]:
        return self.op(self, other, lambda x, y: x | y)

    def __iadd__(self, other: Tree[_LeafT]) -> Tree[_LeafT]:
        return self.iop(other, lambda x, y: x + y)

    def __add__(self, other: Tree[_LeafT]) -> Tree[_LeafT]:
        return self.op(self, other, lambda x, y: x + y)

    def from_dict(self, tree: dict, depth: int = float("inf")):
        for k, v in tree.items():
            if isinstance(v, dict) and depth > 1:
                self[(k,)].from_dict(v, depth=depth - 1)
            else:
                self[k] = self.leaf(v) if self.leaf else v
        return self
