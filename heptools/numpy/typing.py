from typing import TypedDict


class NDArrayBase64(TypedDict):
    dtype: str
    shape: tuple[int, ...]
    data: str
