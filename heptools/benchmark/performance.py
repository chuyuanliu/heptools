from __future__ import annotations

import time
import tracemalloc

from coffea.processor.accumulator import accumulate


class Performance:
    '''
        - current memory [B]
        - peak memory [B]
        - wall time [s]
        - cpu time [s]
    '''
    def __init__(self, *meta, **raw: list[tuple[int, int, float, float]]):
        self._meta = meta # TODO add meta data
        self._raw = raw

    def reset(self):
        self._perf_start = time.perf_counter()
        self._proc_start = time.process_time()
        tracemalloc.clear_traces()

    def checkpoint(self, name: str):
        self._raw.setdefault(name, []).append(
            (*tracemalloc.get_traced_memory(),
             time.perf_counter() - self._perf_start,
             time.process_time() - self._proc_start))
        self.reset()

    def __add__(self, other: Performance):
        if isinstance(other, Performance):
            return Performance(*self._meta, **accumulate(self._raw, other._raw)) # TODO fix and test
        else:
            return NotImplemented

    def __str__(self):
        pass # TODO

    def start(self):
        tracemalloc.start()
        self.reset()

    def stop(self):
        tracemalloc.stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()        