import json

import awkward as ak

from ..utils.json import DefaultEncoder


def jsonable(*jsonables) -> ak.Array:
    return ak.Array(json.loads(json.dumps(jsonables, cls=DefaultEncoder)))
