# TODO docstring

import json
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class JSONable(Protocol):
    def __json__(self) -> Any:
        ...


class DefaultEncoder(json.JSONEncoder):
    def default(self, __obj):
        if isinstance(__obj, JSONable):
            return __obj.__json__()
        return super().default(__obj)
