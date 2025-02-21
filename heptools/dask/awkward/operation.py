import awkward as ak

from ._meta import no_touch_first_array
from .wrapper import delayed

# operations

array = delayed(typehint=ak.Array)(ak.Array)
to_backend = delayed(typehint=ak.to_backend, meta=no_touch_first_array)(ak.to_backend)
