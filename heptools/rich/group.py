from threading import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rich.console import RenderableType


class UniqueGroup:
    def __init__(self):
        self._renderables: dict[int, RenderableType] = {}
        self._lock = Lock()

    def add(self, *renderables: RenderableType):
        with self._lock:
            for r in renderables:
                r_id = id(r)
                if r_id not in self._renderables:
                    self._renderables[r_id] = r

    def remove(self, *renderables: RenderableType):
        with self._lock:
            for renderable in renderables:
                self._renderables.pop(id(renderable), None)

    def __rich_console__(self, console, options):
        with self._lock:
            for renderable in self._renderables.values():
                yield renderable
