# TODO deprecated
from __future__ import annotations

import random
import string
import time
import tracemalloc
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import Callable

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import numpy.typing as npt

from .unit import Binary, Metric, Prefix

matplotlib.rc('font', size = 15)

class Performance:
    measures: list[tuple[str, Prefix, str]] = [
        ('memory', Binary, 'B'),
        ('peak memory', Binary, 'B'),
        ('wall time', Metric, 's'),
        ('cpu time', Metric, 's')]
    width = 20

    def __init__(self, group: str = None):
        self._raw = defaultdict(list)
        if group is None:
            group = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(self.width - 1))
        self._group = group

    def reset(self):
        self._perf_start = time.perf_counter()
        self._proc_start = time.process_time()
        tracemalloc.clear_traces()

    def checkpoint(self, name: str, group: str = None):
        self._raw[name].append(
            (*tracemalloc.get_traced_memory(),
             time.perf_counter() - self._perf_start,
             time.process_time() - self._proc_start,
             self._group if group is None else group))
        self.reset()

    def __add__(self, other: Performance) -> Performance:
        if isinstance(other, Performance):
            new = Performance()
            for raw in (self._raw, other._raw):
                for k, v in raw.items():
                    new._raw[k] += v
            return new
        return NotImplemented

    def analyze(self, **ops: Callable[[npt.NDArray], float]):
        def tree1(): return defaultdict(float)
        def tree2(): return defaultdict(tree1)
        def tree3(): return defaultdict(tree2)
        data = tree3()
        data[None] = set()
        for k, v in self._raw.items():
            for i, measure in enumerate(self.measures):
                measure = measure[0]
                data[k][measure]['raw'] = np.fromiter((x[i] for x in v), dtype = np.float_)
                for op_name, op in ops.items():
                    data[ k][measure][op_name]  = op(data[k][measure]['raw'])
                    data[''][measure][op_name] += data[k][measure][op_name]
            data[k][None] = [x[-1] for x in v]
            data[None] |= set(data[k][None])
        self.width = Performance.width
        for k in chain(data, data[None]):
            if isinstance(k, str):
                if len(k) + 1 > self.width:
                    self.width = len(k) + 1
        return data

    def report(self, *checkpoints: str, **ops: Callable[[npt.NDArray], float]):
        def get_cell(v: float):
            v, u = prefix.add(v)
            return f'{v:.3g}{u}{unit}'
        def sep(c):
            return f'\n{c*(self.width*columns)}\n'
        data = self.analyze(**ops)
        checkpoints = (sorted([k for k in data if k]) if len(checkpoints) == 0 else [*checkpoints]) + ['']
        columns = max(len(ops) + 1, len(checkpoints))
        report = ''
        for measure, prefix, unit in self.measures:
            report += f'{sep("=")}{measure}:\n{"":<{self.width}}'
            for op_name in ops:
                report += f'{op_name:<{self.width}}'
            for k in checkpoints:
                report += f'\n{k:<{self.width}}'
                for op_name in ops:
                    report += f'{get_cell(data[k][measure][op_name]):<{self.width}}'
            report += f'{sep("-")}{"":<{self.width}}'
            for k in checkpoints:
                report += f'{k:<{self.width}}'
            for group in sorted(data[None]):
                report += f'\n{group:<{self.width}}'
                for k in checkpoints:
                    cell = get_cell(data[k][measure]['raw'][data[k][None].index(group)]) if group in data[k][None] else ''
                    report += f'{cell:<{self.width}}'
        report += sep("=")
        return report

    def plot(self, *checkpoints: str, path: str = None, **ops: Callable[[npt.NDArray], float]):
        def pathify(s: str):
            return s.replace(' ', '_')
        data = self.analyze(**ops)
        checkpoints = (sorted([k for k in data if k]) if len(checkpoints) == 0 else [*checkpoints])
        if path is not None:
            for measure, _, _ in self.measures:
                Path(path).joinpath(pathify(measure)).mkdir(parents=True, exist_ok=True)
            path = f'{path}/{{measure}}/{{fig}}.pdf'
            def finish(fig, measure):
                plt.savefig(path.format(measure = pathify(measure), fig = pathify(fig)))
                plt.close()
        else:
            def finish(fig, measure):
                plt.show()
        groups = sorted(data[None])
        for measure, _, _ in self.measures:
            for op_name in ops:
                plt.figure(figsize = (8, 8))
                y = [data[k][measure][op_name] for k in checkpoints]
                plt.pie(y, labels = checkpoints, autopct = '%1.1f%%')
                plt.title(f'{measure}({op_name})')
                finish(op_name, measure)
            plt.figure(figsize = (2 * len(groups) + 6, 8))
            bottom = np.zeros(len(groups))
            for k in checkpoints:
                height = np.array([data[k][measure]['raw'][data[k][None].index(group)] if group in data[k][None] else 0 for group in groups])
                plt.bar(np.arange(len(groups)), height = height, width = 0.5, label = k, bottom = bottom)
                bottom += height
            plt.xticks(np.arange(len(groups)), groups)
            plt.title(f'{measure}(raw)')
            plt.legend()
            finish('raw', measure)

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