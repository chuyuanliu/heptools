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
        keep: set[str] = set(ak.fields(data))
        to_zip: dict[str, set[str]] = {}
        if self._cache is not None:
            key = frozenset(keep)
            if key in self._cache:
                return self._cache[key]
        if self._jagged or self._regular:
            raw = np.array(ak.fields(data))
            pair = np.char.partition(raw, '_')[:, :2]
            jagged = pair[pair[:, 1] == ''][:, 0]
            jagged = jagged[np.char.startswith(jagged, 'n')]
            jagged_prefix = np.char.lstrip(jagged, 'n')
            jagged, jagged_prefix = set(jagged), set(jagged_prefix)
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
                keep -= jagged
                for prefix in jagged_prefix:
                    if prefix in pair:
                        to_zip[prefix] = set(pair[prefix])
            if self._regular:
                for prefix in set(pair) - jagged_prefix:
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
            kept = data[keep]
            zipped = dict(zip(ak.fields(kept), ak.unzip(kept)))
            for k, vs in to_zip.items():
                start = len(k) + 1
                zipped[k] = ak.zip({v[start:]: data[v] for v in vs})
            return ak.Array(zipped)
