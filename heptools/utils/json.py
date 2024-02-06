import json
from typing import Protocol, runtime_checkable


@runtime_checkable
class JSONable(Protocol):
    def to_json(self):
        ...


class DefaultEncoder(json.JSONEncoder):
    def default(self, __obj):
        if isinstance(__obj, JSONable):
            return __obj.to_json()
        return super().default(__obj)
