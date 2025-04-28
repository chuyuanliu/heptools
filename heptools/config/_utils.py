from __future__ import annotations


class SimpleTree:
    @classmethod
    def init(cls, tree: dict, keys: tuple[str, ...]):
        for key in keys:
            if key not in tree:
                tree[key] = {}
            tree = tree[key]
        return tree

    @classmethod
    def set(cls, tree: dict, keys: tuple[str, ...], value):
        cls.init(tree, keys[:-1])[keys[-1]] = value

    @classmethod
    def get(cls, tree: dict, keys: tuple[str, ...], default=None):
        for key in keys:
            if key not in tree:
                return default
            tree = tree[key]
        return tree
