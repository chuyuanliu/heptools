from __future__ import annotations

import time
from copy import deepcopy
from functools import partial
from threading import Lock, Thread

import psutil
from bokeh.document import Document
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, CrosshairTool, HoverTool, Span
from bokeh.plotting import figure

_TIME = "time"
_MEMORY = "memory"
_CPU = "cpu"
_NAME = "name"

_BASIC = (_TIME, _MEMORY, _CPU)
_TITLE = {
    _MEMORY: "Memory",
    _CPU: "CPU",
}
_LABEL = {
    _TIME: "Time (s)",
    _MEMORY: f"Memory (MB)",
    _CPU: "CPU (%)",
}

_FIGURE_KWARGS = {
    "tools": "xpan,xwheel_zoom,reset",
    "height": 300,
    "sizing_mode": "stretch_width",
}
_VSPAN_WIDTH = 2
_CIRCLE_SIZE = 10
_X_RANGE = 100


def _init(*key):
    return {k: [] for k in _BASIC + key}


def _append(record: dict[str, list], *items: dict[str]):
    for item in items:
        for k, v in item.items():
            record[k].append(v)
    return record


def _slice(record: dict[str, list], start: int, end: int):
    return {k: v[start:end] for k, v in record.items()}


class ResourceTracker:
    fps: int = 4

    _t: ResourceTracker = None
    _p = psutil.Process()

    def __init__(self, period: float = 0.1):
        if self.__class__._t is not None:
            raise RuntimeError("ResourceTracker is singleton")
        self.__class__._t = self

        self._period = period

        self._start_t = self._p.create_time()
        self._records = _init()
        self._checkpoints = _init(_NAME)

        self._running = True
        self._lock = Lock()
        self._thread = Thread(target=self._period_update, daemon=True)

    @classmethod
    def start(cls):
        self = cls._t
        if self is None:
            return
        self._thread.start()
        self.checkpoint("start")

    @classmethod
    def stop(cls):
        self = cls._t
        if self is None:
            return
        self.checkpoint("stop")
        self._running = False
        self._thread.join()
        cls._t = None

    @classmethod
    def checkpoint(cls, name):
        self = cls._t
        if self is None:
            return
        with self._lock:
            _append(self._checkpoints, self._resource_query(), {_NAME: name})

    def _resource_query(self):
        return {
            _TIME: time.time() - self._start_t,
            _MEMORY: self._p.memory_info().rss / 1024**2,
            _CPU: self._p.cpu_percent(interval=self._period),
        }

    def _period_update(self):
        while self._running:
            with self._lock:
                _append(self._records, self._resource_query())
            time.sleep(self._period)

    @classmethod
    def doc(cls, doc: Document):
        self = cls._t
        if self is None:
            r_source = _init()
            c_source = _init(_NAME)
        else:
            with self._lock:
                r_source = deepcopy(self._records)
                c_source = deepcopy(self._checkpoints)
        r_source = ColumnDataSource(data=r_source)
        c_source = ColumnDataSource(data=c_source)

        tooltip = HoverTool(
            tooltips=[
                (_TIME, f"@{_TIME} s"),
                (_MEMORY, f"@{_MEMORY} MB"),
                (_CPU, f"@{_CPU}%"),
                (_NAME, f"@{_NAME}"),
            ],
        )
        tooltip.renderers = []
        crosshair = CrosshairTool(
            dimensions="height",
            overlay=Span(
                dimension="height", line_dash="dashed", line_width=_VSPAN_WIDTH
            ),
        )
        figs = []

        for key, title in _TITLE.items():
            p: figure = figure(
                title=title,
                x_axis_label=_LABEL[_TIME],
                y_axis_label=_LABEL[key],
                **_FIGURE_KWARGS,
            )
            if figs:
                p.x_range = figs[0].x_range
            else:
                p.x_range.follow = "end"
                p.x_range.follow_interval = _X_RANGE
            p.add_tools(tooltip, crosshair)
            p.line(x=_TIME, y=key, source=r_source)
            tooltip.renderers.append(
                p.scatter(
                    x=_TIME, y=key, source=c_source, size=_CIRCLE_SIZE, color="red"
                )
            )
            figs.append(p)

        layout = column(*figs, sizing_mode="stretch_width")
        if doc is None:
            return layout
        doc.add_root(layout)
        doc.add_periodic_callback(
            partial(cls._bk_update, r_source, c_source), 1000 // cls.fps
        )
        return doc

    @classmethod
    def _bk_update(cls, r: ColumnDataSource, c: ColumnDataSource):
        self = cls._t
        if self is None:
            return
        c_start = len(c.data[_TIME])
        c_end = len(self._checkpoints[_TIME])
        c.stream(_slice(self._checkpoints, c_start, c_end))

        r_start = len(r.data[_TIME])
        r_end = len(self._records[_TIME])
        r.stream(_slice(self._records, r_start, r_end))
