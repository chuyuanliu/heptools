import awkward as ak

from .wrapper import delayed


def _to_backend_meta(array, *_, **__):
    return array


# operations

array = delayed(typehint=ak.Array)(ak.Array)
to_backend = delayed(typehint=ak.to_backend, meta=_to_backend_meta)(ak.to_backend)
