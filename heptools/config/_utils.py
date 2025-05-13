from __future__ import annotations

from textwrap import indent


def format_repr(value, maxlines: int = None) -> str:
    if isinstance(value, dict):
        lines = []
        for k, v in value.items():
            line = format_repr(v)
            if isinstance(v, (dict, list)) or line.count("\n") > 0:
                line = f"{k}:\n{indent(line, '  ')}"
            else:
                line = f"{k}: {line}"
            lines.append(line)
        text = "\n".join(lines)
    elif isinstance(value, list):
        text = "\n".join("- " + format_repr(v).replace("\n", "\n  ") for v in value)
    elif isinstance(value, (str, int, float, bool, type(None))):
        text = str(value)
    else:
        text = format_repr(value)
    if maxlines is not None:
        lines = text.split("\n")
        if len(lines) > maxlines:
            lines[maxlines - 1] = f"+ {len(lines) - maxlines + 1} more lines"
        text = "\n".join(lines[:maxlines])
    return text


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
