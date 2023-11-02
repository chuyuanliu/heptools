from __future__ import annotations

import re
from copy import deepcopy
from typing import Callable, Iterable, overload

import numpy as np
from hist import Hist
from hist.axis import (AxesMixin, Boolean, IntCategory, Integer, Regular,
                       StrCategory, Variable)

from ..aktools import AnyInt, FieldLike, RealNumber, get_field
from ..typetools import check_type
from ..utils import astuple, match_any
from . import fill as fs

AxisLike = AxesMixin | tuple

class HistError(Exception):
    __module__ = Exception.__module__

def _default_field(_s: str):
    return (*_s.split('.'),)

def _create_axis(args: AxisLike) -> AxesMixin:
    if isinstance(args, AxesMixin):
        return deepcopy(args)
    if len(args) == 0:
        raise HistError('require at least one argument "name" to create an axis')
    label = Label(args[-1]).askwarg('name', 'label')
    if len(args) == 4:
        if check_type(args[0], AnyInt) and args[0] > 0 and all(check_type(arg, RealNumber) for arg in args[1:3]):
            return Regular(*args[0:3], **label)
    elif len(args) == 3:
        if all(check_type(arg, AnyInt) for arg in args[0:2]) and args[0] <= args [1]:
            return Integer(*args[0:2], **label)
    elif len(args) == 2:
        if args[0] is ...:
            return Boolean(**label)
        elif isinstance(args[0], Iterable):
            if all(isinstance(arg, str) for arg in args[0]):
                return StrCategory(args[0], **label, growth = True)
            elif all(check_type(arg, AnyInt) for arg in args[0]):
                return IntCategory(args[0], **label, growth = True)
            elif all(check_type(arg, RealNumber) for arg in args[0]):
                return Variable(args[0], **label)
    elif len(args) == 1:
        return Boolean(**label)
    raise HistError(f'cannot create axis from arguments "{args}"')

class Label:
    @overload
    def __init__(self, label: LabelLike):
        ...
    @overload
    def __init__(self, code: str, display: str):
        ...
    def __init__(self, code: LabelLike, display: str = ...):
        if isinstance(code, Label):
            self.code    = code.code
            self.display = code.display
        elif isinstance(code, tuple):
            self.code    = code[0]
            self.display = code[1]
        elif isinstance(code, str):
            self.code    = code
            self.display = code if display is ... else display

    def askwarg(self, code: str = 'code', display: str = 'display'):
        return {code: self.code, display: self.display}

    def __repr__(self) -> str: # TODO __repr__
        return f'Label({self.code}, {self.display})'

LabelLike = str | tuple[str, str] | Label

class Collection:
    current: Collection = None

    def __init__(self, **categories):
        self._fills: dict[str, list[str]] = {}
        self._hists: dict[str,   Hist   ] = {}
        self._categories = deepcopy(categories)
        self._axes:  dict[str, AxesMixin] = {k: _create_axis((*(v if isinstance(v, tuple) else (v,)), k)) for k, v in self._categories.items()}
        self.cd()

    def add(self, name: str, *axes: AxisLike, **fill_args: fs.FillLike):
        axes = [_create_axis(axis) for axis in axes]
        self._fills[name] = [_axis.name for _axis in axes]
        self._hists[name] = Hist(*self._axes.values(), *axes, storage = 'weight', label = 'Events')
        return self.auto_fill(name, **fill_args)

    def _generate_category_combinations(self, categories: list[str]) -> list[dict[str, str]]:
        if len(categories) == 0:
            return [{}]
        else:
            combs = np.stack(np.meshgrid(*(self._categories[category] for category in categories)), axis = -1).reshape((-1, len(categories)))
            return [dict(zip(categories, comb)) for comb in combs]

    def auto_fill(self, name: str, **fill_args: fs.FillLike):
        default_args = {k: _default_field(k) for k in self._fills[name] if k not in fill_args}
        fill_args = {f'{name}:{k}': v for k, v in fill_args.items()} | default_args
        fills = {name: self._fills[name] + [*self._categories] + ['weight']}
        return fs.Fill(fills, **fill_args)

    def duplicate_axes(self, name: str) -> list[AxesMixin]:
        axes = []
        if name in self._hists:
            for axis in self._hists[name].axes:
                if axis not in self._axes.values():
                    axes.append(deepcopy(axis))
        return axes

    def cd(self):
        Collection.current = self

    @property
    def output(self):
        return {'hists': self._hists, 'categories': {*self._categories}}

class Template:
    class _Hist:
        def __init__(self, *axes: AxisLike, **fill_args: fs.LazyFill):
            self._axes = [(
                Label(axis.name, axis.label) if isinstance(axis, AxesMixin) else Label(axis[-1])
                , axis) for axis in axes
            ]
            self.fill_args = fill_args

        def axes(self, name: str, bins: dict[str | tuple[str, str], AxisLike]) -> list[AxisLike]:
            _axes = []
            for label, axis in self._axes:
                args = bins.get((name, label.code), bins.get(label.code))
                if args is None:
                    _axes.append(axis)
                else:
                    _axes.append((*args, label))
            return _axes

        def __repr__(self): # TODO __repr__
            return ', '.join(str(axis) for _, axis in self._axes)

    def __init__(
            self,
            name: LabelLike,
            fill: FieldLike = ...,
            bins: dict[str | tuple[str, str], AxisLike] = None,
            skip: Iterable[str | re.Pattern] = None,
            **fill_args: fs.LazyFill):
        self._fills = fs.Fill()
        self._name  = Label(name)
        self._data  = astuple(_default_field(self._name.code) if fill is ... else fill)
        self._fill_args = fill_args

        if bins is None:
            bins = {}
        for name, hist in self.hists().items():
            if not match_any(name, skip, lambda x, y: re.match(y, x) is not None):
                self._add(name, *hist.axes(name, bins), **hist.fill_args)

    def _add(self, name: str, *axes: AxisLike, **fill_args: fs.LazyFill):
        _kwargs = {}
        fill_args = self._fill_args | fill_args
        axes = [_create_axis(axis) for axis in axes]
        for axis in axes:
            axis.label = f'{self._name.display} {axis.label}'
            if axis.name in fill_args:
                _fill = fill_args[axis.name]
                if check_type(_fill, FieldLike):
                    _fill = self._data + astuple(_fill)
                elif check_type(_fill, Callable):
                    _fill = self._wrap(_fill)
                _kwargs[axis.name] = _fill
            else:
                _kwargs[axis.name] = self._data + _default_field(axis.name)
        if 'weight' in fill_args:
            _kwargs['weight'] = fill_args['weight']
        self._fills += Collection.current.add(f'{self._name.code}.{name}', *axes, **_kwargs)

    def _wrap(self, func: Callable):
        return lambda x: func(get_field(x, self._data))

    @classmethod
    def hists(cls) -> dict[str, _Hist]:
        hists = {}
        for name in dir(cls):
            attr = getattr(cls, name)
            if isinstance(attr, cls._Hist):
                hists[name] = attr
        return hists

    def __new__(cls, *args, **kwargs):
        self = object.__new__(cls)
        self.__init__(*args, **kwargs)
        return self._fills

class Systematic(Template):
    def __init__(self, name: str, systs: Iterable[LabelLike], *axes: AxesMixin | tuple, weight: FieldLike = 'weight', **fill_args: FieldLike):
        super().__init__((name, ''), (), **fill_args)
        weight = astuple(weight)
        if len(axes) == 0:
            axes = Collection.current.duplicate_axes(name)
        for _var in systs:
            _var = Label(_var)
            self._name.display = f'({_var.display})'
            self._add(_var.code, *axes, weight = weight + _default_field(_var.code))
        self._name.display = ''
