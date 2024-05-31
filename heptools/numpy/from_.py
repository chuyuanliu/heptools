from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import numpy.typing as npt

    from .typing import NDArrayBase64


def base64(d: NDArrayBase64) -> npt.NDArray:
    import base64

    return np.frombuffer(
        base64.b64decode(d["data"].encode()), dtype=d["dtype"]
    ).reshape(*d["shape"])
