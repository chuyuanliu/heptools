from __future__ import annotations

from copy import deepcopy
from typing import Iterable, Union

import numpy as np
from hist import Hist
from hist.axis import (AxesMixin, Boolean, IntCategory, Integer, Regular,
                       StrCategory, Variable)
from hist.storage import Storage

from .._utils import astuple, isinstance_
from ..aktools import AnyInt, AnyNumber, FieldLike
from . import fill as fs


class HistError(Exception):
    __module__ = Exception.__module__

def _default_field(_s: str):
    return (*_s.split('.'),)

def _create_axis(args: AxesMixin | tuple) -> AxesMixin:
    if isinstance(args, AxesMixin):
        return deepcopy(args)
    assert len(args) > 0
    label = Label(args[-1]).askwarg('name', 'label')
    if len(args) == 4:
        if isinstance_(args[0], AnyInt) and args[0] > 0 and all(isinstance_(arg, AnyNumber) for arg in args[1:3]):
            return Regular(*args[0:3], **label)
    elif len(args) == 3:
        if all(isinstance_(arg, AnyInt) for arg in args[0:2]) and args[0] <= args [1]:
            return Integer(*args[0:2], **label)
    elif len(args) == 2:
        if args[0] is ...:
            return Boolean(**label)
        elif isinstance(args[0], Iterable):
            if all(isinstance(arg, str) for arg in args[0]):
                return StrCategory(args[0], **label, growth = True)
            elif all(isinstance_(arg, AnyInt) for arg in args[0]):
                return IntCategory(args[0], **label, growth = True)
            elif all(isinstance_(arg, AnyNumber) for arg in args[0]):
                return Variable(args[0], **label)
    elif len(args) == 1:
        return Boolean(**label)
    raise HistError(f'cannot create axis from arguments "{args}"')

class Label:
    def __init__(self, code: LabelLike, display: str = None):
        if isinstance(code, Label):
            self.code    = code.code
            self.display = code.display
        elif isinstance(code, tuple):
            self.code    = code[0]
            self.display = code[1]
        elif isinstance(code, str):
            self.code    = code
            self.display = code if display is None else display

    def askwarg(self, code: str = 'code', display: str = 'display'):
        return {code: self.code, display: self.display}

LabelLike = Union[str, tuple[str, str], Label]

class Set:
    current: Set = None
    def __init__(self, **categories):
        self._fills: dict[str, list[str]] = {}
        self._hists: dict[str,   Hist   ] = {}
        self._categories = deepcopy(categories)
        self._axes:  dict[str, AxesMixin] = dict((k, _create_axis((v, (k, k.capitalize())))) for k, v in self._categories.items())
        self.focus()

    def add(self, name: str, *axes: AxesMixin | tuple, storage: str | Storage = 'weight', label: str = 'Events', **fill_args: fs.FillLike):
        axes = [_create_axis(axis) for axis in axes]
        self._fills[name] = [_axis.name for _axis in axes]
        self._hists[name] = Hist(*self._axes.values(), *axes, storage = storage, label = label)
        return self.auto_fill(name, **fill_args)

    def _generate_category_combinations(self, categories: list[str]) -> list[dict[str, str]]:
        if len(categories) == 0:
            return [{}]
        else:
            combs = np.stack(np.meshgrid(*(self._categories[category] for category in categories)), axis = -1).reshape((-1, len(categories)))
            return [dict(zip(categories, comb)) for comb in combs]

    def auto_fill(self, name: str, **fill_args: fs.FillLike):
        default_args = dict((k, _default_field(k)) for k in filter(lambda x: x not in fill_args, self._fills[name]))
        fill_args = dict((f'{name}:{k}', v) for k, v in fill_args.items()) | default_args
        fills = {name: self._fills[name] + list(self._categories) + ['weight']}
        return fs.Fill(fills, **fill_args)

    def duplicate_axes(self, name: str) -> list[AxesMixin]:
        axes = []
        if name in self._hists:
            for axis in self._hists[name].axes:
                if axis not in self._axes.values():
                    axes.append(deepcopy(axis))
        return axes

    def focus(self):
        Set.current = self

    @property
    def output(self):
        return {'hists': self._hists, 'categories': {*self._categories}}

class Subset:
    def __init__(self, name: LabelLike, fill: FieldLike = None, **fill_args: FieldLike):
        self._fills = fs.Fill()
        self._name  = Label(name)
        self._data  = astuple(_default_field(self._name.code) if fill is None else fill)
        self._fill_args = fill_args

    def add(self, name: str, *axes: AxesMixin | tuple, storage: str | Storage = 'weight', label: str = 'Events', **fill_args: FieldLike):
        _fill = {}
        fill_args = self._fill_args | fill_args
        axes = [_create_axis(axis) for axis in axes]
        for axis in axes:
            axis.label = f'{self._name.display} {axis.label}'
            if axis.name in fill_args:
                if isinstance_(fill_args, FieldLike):
                    _fill[axis.name] = self._data + astuple(fill_args[axis.name])
                else:
                    _fill[axis.name] = fill_args[axis.name]
            else:
                _fill[axis.name] = self._data + _default_field(axis.name)
        if 'weight' in fill_args:
            _fill['weight'] = fill_args['weight']
        self._fills += Set.current.add(f'{self._name.code}.{name}', *axes, storage = storage, label = label, **_fill)

    def auto_fill(self) -> fs.Fill:
        return self._fills

    def __new__(cls, *args, **kwargs):
        self = object.__new__(cls)
        self.__init__(*args, **kwargs)
        return self._fills