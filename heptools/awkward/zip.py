# TODO docstring
import awkward as ak
import numpy as np


class NanoAOD:
    def __init__(
            self,
            regular: bool = True,
            jagged: bool = True,
            cache: bool = True):
        self._regular = regular
        self._jagged = jagged
        self._cache: dict[frozenset[str],
                          tuple[set[str], dict[str, set[str]]]] = {} if cache else None

    def _parse_fields(self, data: ak.Array):
        keep: set[str] = set(data.fields)
        to_zip: dict[str, set[str]] = {}
        if self._cache is not None:
            key = frozenset(keep)
            if key in self._cache:
                return self._cache[key]
        if self._jagged or self._regular:
            raw = np.array(data.fields)
            pair = np.char.partition(raw, '_')[:, :2]
            single: set[str] = set(pair[pair[:, 1] == ''][:, 0])
            _select = pair[:, 1] == '_'
            pair, raw = pair[_select][:, 0], raw[_select]
            _select = pair.argsort()
            pair, raw = pair[_select], raw[_select]
            del _select
            pair_key, pair_idx = np.unique(pair, return_index=True)
            pair: dict[str, list[str]] = dict(
                zip(pair_key, np.split(raw, pair_idx[1:])))
            del pair_key, pair_idx
            if self._jagged:
                jagged = {b for b in single if b.startswith('n')}
                keep -= jagged
                for prefix in jagged:
                    prefix = prefix[1:]
                    if prefix in pair:
                        to_zip[prefix] = set(pair[prefix])
                        del pair[prefix]
            if self._regular:
                for prefix in pair:
                    to_zip[prefix] = set(pair[prefix])
            for v in to_zip.values():
                keep -= v
        if self._cache is not None:
            self._cache[key] = keep, to_zip
        return keep, to_zip

    def __call__(self, data: ak.Array):
        keep, to_zip = self._parse_fields(data)
        if not to_zip:
            return data
        else:
            zipped = {}
            for k in keep:
                zipped[k] = data[k]
            for k, vs in to_zip.items():
                start = len(k) + 1
                zipped[k] = ak.zip({v[start:]: data[v] for v in vs})
            return ak.Array(zipped)
