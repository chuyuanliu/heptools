from threading import Thread
from typing import Protocol

import fsspec
from bokeh.document import Document
from bokeh.embed import file_html
from bokeh.resources import CDN, INLINE
from bokeh.server.server import Server
from tornado.ioloop import IOLoop

from ...system.eos import EOS, PathLike


class Component(Protocol):
    @classmethod
    def doc(cls, doc: Document): ...

    @classmethod
    def start(cls): ...

    @classmethod
    def stop(cls): ...


class Tracker:
    def __init__(self, port: int = 5006):
        self._port = port
        self._components: dict[str, type[Component]] = {}
        self._server: Server = None
        self._thread: Thread = None

    def add(self, component: Component | type[Component]):
        if not isinstance(component, type):
            component = type(component)
        name = component.__name__
        if name in self._components:
            raise ValueError(f"Component {name} already exists")
        self._components[f"{name}"] = component

    def start(self):
        if self._server is None:
            for component in self._components.values():
                component.start()
            self._server = Server(
                {f"/{k}": v.doc for k, v in self._components.items()},
                port=self._port,
                io_loop=IOLoop(),
            )
            self._server.start()
            self._server.io_loop.add_callback(self._server.show, "/")
            self._thread = Thread(target=self._server.io_loop.start, daemon=True)
            self._thread.start()

    def stop(self):
        if self._server is not None:
            self._server.stop()
            self._server.io_loop.stop()
            self._thread.join()
            self._server = None
            self._thread = None
            for component in self._components.values():
                component.stop()

    def dump(self, output: PathLike, inline_resources: bool = False):
        resource = INLINE if inline_resources else CDN
        output = EOS(output).mkdir(True)
        for name, component in self._components.items():
            with fsspec.open(output / f"{name}.html", "w") as file:
                file.write(
                    file_html(component.doc(None), title=name, resources=resource)
                )
