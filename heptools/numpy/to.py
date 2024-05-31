from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy.typing as npt

    from .typing import NDArrayBase64


def base64(a: npt.NDArray) -> NDArrayBase64:
    import base64

    return {
        "dtype": a.dtype.name,
        "shape": a.shape,
        "data": base64.b64encode(a).decode(),
    }
