import json
from typing import TypeVar

import awkward as ak

from ..utils.json import DefaultEncoder

JSONableT = TypeVar('JSONableT')


def from_jsonable(*jsonables) -> ak.Array:
    return ak.Array(json.loads(json.dumps(jsonables, cls=DefaultEncoder)))


def to_jsonable(array: ak.Array, cls: type[JSONableT]) -> list[JSONableT]:
    array = ak.to_list(array)
    if hasattr(cls, 'from_json'):
        return [cls.from_json(data) for data in array]
    else:
        return [cls(**data) for data in array]
