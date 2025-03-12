from __future__ import annotations

from functools import cache
from urllib.parse import ParseResult, urlparse, unquote
from pathlib import Path
from typing import Any
from os import PathLike, fspath


ConfigSource = str | PathLike | dict[str, Any]


@cache
def _read_file(url: str) -> str:
    import fsspec

    with fsspec.open(url, mode="rt") as f:
        data = f.read()
    return data


def _parse_file(url: str | PathLike):
    parsed = urlparse(fspath(url))
    path = unquote(parsed.path)
    data = _read_file(
        ParseResult(parsed.scheme, parsed.netloc, path, "", "", "").geturl()
    )
    match suffix := Path(path).suffix:
        case ".json":
            import json

            data = json.loads(data)
        case ".yaml" | ".yml":
            import yaml

            data = yaml.safe_load(data)
        case _:
            raise NotImplementedError(f"Unsupported file type: {suffix}")

    if parsed.fragment:
        for k in parsed.fragment.split("."):
            data = data[k]

    return data


def _flat_dict(data: dict[str, Any], delimiter: str = ".") -> dict[str, Any]:
    result = {}
    stack = [([k], v) for k, v in data.items()]
    while stack:
        k, v = stack.pop()
        if isinstance(v, dict):
            stack.extend((k + [k2], v2) for k2, v2 in v.items())
        else:
            result[delimiter.join(k)] = v
    return result


def parse_config(*path_or_dict: ConfigSource) -> dict[str, Any]:
    result = {}
    for k in path_or_dict:
        if not isinstance(k, dict):
            k = _parse_file(k)
        result.update(_flat_dict(k))
    return result
