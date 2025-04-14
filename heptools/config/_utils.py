from typing import Iterable


class NestedTree:
    @classmethod
    def init(cls, tree: dict, keys: Iterable[str]):
        for key in keys:
            if key not in tree:
                tree[key] = {}
            tree = tree[key]
        return tree

    @classmethod
    def set(cls, tree: dict, keys: Iterable[str], value):
        cls.init(tree, keys[:-1])[keys[-1]] = value

    @classmethod
    def get(cls, tree: dict, keys: Iterable[str]):
        for key in keys:
            tree = tree[key]
        return tree

    @classmethod
    def has(cls, tree: dict, keys: Iterable[str]):
        for key in keys:
            if key not in tree:
                return False
            tree = tree[key]
        return True
