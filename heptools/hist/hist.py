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
from ..utils import astuple, match_any, re_match_whole
from . import fill as fs


class Label:
    @overload
    def __init__(self, label: LabelLike):
        ...

    @overload
    def __init__(self, code: str, display: str):
        ...

    def __init__(self, code: LabelLike, display: str = ...):
        if isinstance(code, Label):
            self.code = code.code
            self.display = code.display
        elif isinstance(code, tuple):
            self.code = code[0]
            self.display = code[1]
        elif isinstance(code, str):
            self.code = code
            self.display = code if display is ... else display

    def askwarg(self, code: str = 'code', display: str = 'display'):
        return {code: self.code, display: self.display}

    def __repr__(self) -> str:  # TODO __repr__
        return f'Label({self.code}, {self.display})'


LabelLike = str | tuple[str, str] | Label

RegularArgs = tuple[int, RealNumber, RealNumber]
RegularAxis = tuple[int, RealNumber, RealNumber, LabelLike] | Regular
IntegerArgs = tuple[int, int]
IntegerAxis = tuple[int, int, LabelLike] | Integer
BooleanArgs = tuple[Ellipsis] | type[Ellipsis]
BooleanAxis = tuple[Ellipsis, LabelLike] | tuple[LabelLike] | Boolean
StrCategoryArgs = tuple[Iterable[str]]
StrCategoryAxis = tuple[Iterable[str], LabelLike] | StrCategory
IntCategoryArgs = tuple[Iterable[int]]
IntCategoryAxis = tuple[Iterable[int], LabelLike] | IntCategory
VariableArgs = tuple[Iterable[RealNumber]]
VariableAxis = tuple[Iterable[RealNumber], LabelLike] | Variable

AxisArgs = RegularArgs | IntegerArgs | BooleanArgs | StrCategoryArgs | IntCategoryArgs | VariableArgs | AxesMixin
AxisLike = RegularAxis | IntegerAxis | BooleanAxis | StrCategoryAxis | IntCategoryAxis | VariableAxis | AxesMixin


class HistError(Exception):
    __module__ = Exception.__module__


def _default_field(_s: str):
    return (*_s.split('.'),)


def _create_axis(args: AxisLike) -> AxesMixin:
    if isinstance(args, AxesMixin):
        return deepcopy(args)
    if len(args) == 0:
        raise HistError(
            'require at least one argument "name" to create an axis')
    label = Label(args[-1]).askwarg('name', 'label')
    if len(args) == 4:
        if check_type(args[0], AnyInt) and args[0] > 0 and all(check_type(arg, RealNumber) for arg in args[1:3]):
            return Regular(*args[0:3], **label)
    elif len(args) == 3:
        if all(check_type(arg, AnyInt) for arg in args[0:2]) and args[0] <= args[1]:
            return Integer(*args[0:2], **label)
    elif len(args) == 2:
        if args[0] is ...:
            return Boolean(**label)
        elif isinstance(args[0], Iterable):
            if all(isinstance(arg, str) for arg in args[0]):
                return StrCategory(args[0], **label, growth=True)
            elif all(check_type(arg, AnyInt) for arg in args[0]):
                return IntCategory(args[0], **label, growth=True)
            elif all(check_type(arg, RealNumber) for arg in args[0]):
                return Variable(args[0], **label)
    elif len(args) == 1:
        return Boolean(**label)
    raise HistError(f'cannot create axis from arguments "{args}"')


class Collection:
    current: Collection = None

    def __init__(self, **categories):
        self._fills: dict[str, list[str]] = {}
        self._hists: dict[str, Hist] = {}
        self._categories = deepcopy(categories)
        self._axes:  dict[str, AxesMixin] = {k: _create_axis(
            (*(v if isinstance(v, tuple) else (v,)), k)) for k, v in self._categories.items()}
        self.cd()

    def add(self, name: str, *axes: AxisLike, **fill_args: fs.FillLike):
        axes = [_create_axis(axis) for axis in axes]
        self._fills[name] = [_axis.name for _axis in axes]
        self._hists[name] = Hist(*self._axes.values(),
                                 *axes, storage='weight', label='Events')
        return self.auto_fill(name, **fill_args)

    def _generate_category_combinations(self, categories: list[str]) -> list[dict[str, str]]:
        if len(categories) == 0:
            return [{}]
        else:
            combs = np.stack(np.meshgrid(
                *(self._categories[category] for category in categories)), axis=-1).reshape((-1, len(categories)))
            return [dict(zip(categories, comb)) for comb in combs]

    def auto_fill(self, name: str, **fill_args: fs.FillLike):
        default_args = {k: _default_field(
            k) for k in self._fills[name] if k not in fill_args}
        fill_args = {f'{name}:{k}': v for k,
                     v in fill_args.items()} | default_args
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
                Label(axis.name, axis.label) if isinstance(axis, AxesMixin) else Label(axis[-1]), axis) for axis in axes
            ]
            self.fill_args = fill_args

        def axes(self, name: str, template: Template) -> list[AxisLike]:
            _axes = []
            for label, axis in self._axes:
                args = template.rebin(name, label.code)
                if args is None:
                    _axes.append(axis)
                else:
                    if isinstance(args, tuple):
                        _axes.append((*args, label))
                    elif args is ...:
                        _axes.append((label,))
                    elif isinstance(args, AxesMixin):
                        args = deepcopy(args)
                        args.label = label.display
                        args.name = label.code
                        _axes.append(args)
            return _axes

        def __repr__(self):  # TODO __repr__
            return ', '.join(str(axis) for _, axis in self._axes)

    def __init__(
            self,
            name: LabelLike,
            fill: FieldLike = ...,
            bins: dict[str | tuple[str, str], AxisArgs] = None,
            skip: Iterable[str | re.Pattern] = None,
            **fill_args: fs.LazyFill):
        self._name = Label(name)
        self._data = fill
        self._bins = bins.copy() if bins is not None else {}
        self._skip = list(skip) if skip is not None else []
        self._fill_args = fill_args

        self._fills = fs.Fill()
        self._created = False
        self._parent: Template = None

    def copy(self):
        return self.__class__(
            self._name,
            self._data,
            self._bins,
            self._skip,
            **self._fill_args
        )

    @property
    def data(self):
        if self._parent is not None:
            return self._parent.data + self._data
        return self._data

    @property
    def fill_args(self):
        if self._parent is not None:
            return self._fill_args | self._parent.fill_args
        return self._fill_args

    def hist_name(self, name: str, nested: bool = False):
        name = f'{self._name.code}.{name}'
        if nested and self._parent is not None:
            return self._parent.hist_name(name, nested)
        return name

    def axis_label(self, label: str):
        label = f'{self._name.display} {label}'
        if self._parent is not None:
            label = self._parent.axis_label(label)
        return label

    def rebin(self, name: str, axis: str):
        _axis = None
        if self._parent is not None:
            _axis = self._parent.rebin(self.hist_name(name), axis)
        if _axis is None:
            _axis = self._bins.get((name, axis), self._bins.get(axis))
        return _axis

    def skip(self, name: str):
        skip = False
        if self._parent is not None:
            skip |= self._parent.skip(self.hist_name(name))
        if not skip:
            skip |= match_any(name, self._skip, re_match_whole)
        return skip

    def new(self, name: str = None, parent: Template = None):
        if not self._created:
            self._created = True
            self._parent = parent
            if name is not None:
                self._name.code = name
            self._data = astuple(
                self._data if self._data is not ... else _default_field(self._name.code))
            hists, templates = self.hists()
            for name, hist in hists.items():
                if not self.skip(name):
                    self._add(name, *hist.axes(name, self), **hist.fill_args)
            for name, template in templates.items():
                if template._created:
                    raise HistError(
                        f'Template "{self.__class__.__name__}.{name}" has already been used')
                template = template.copy()
                self._fills += template.new(name, self)
        return self._fills

    def _add(self, name: str, *axes: AxisLike, **fill_args: fs.LazyFill):
        _kwargs = {}
        fill_args = fill_args | self.fill_args
        axes = [_create_axis(axis) for axis in axes]
        data = self.data
        for axis in axes:
            axis.label = self.axis_label(axis.label)
            if axis.name in fill_args:
                _fill = fill_args[axis.name]
                if check_type(_fill, FieldLike):
                    _fill = data + astuple(_fill)
                elif check_type(_fill, Callable):
                    _fill = self._wrap(_fill)
                _kwargs[axis.name] = _fill
            else:
                _kwargs[axis.name] = data + _default_field(axis.name)
        if 'weight' in fill_args:
            _kwargs['weight'] = fill_args['weight']
        self._fills += Collection.current.add(
            self.hist_name(name, nested=True), *axes, **_kwargs)

    def _wrap(self, func: Callable):
        return lambda x: func(get_field(x, self.data))

    @classmethod
    def hists(cls) -> tuple[dict[str, _Hist], dict[str, Template]]:
        hists, templates = {}, {}
        for name in dir(cls):
            attr = getattr(cls, name)
            if isinstance(attr, cls._Hist):
                hists[name] = attr
            elif isinstance(attr, Template):
                templates[name] = attr
        return hists, templates

    def __add__(self, other: fs.Fill | Template) -> fs.Fill:
        if isinstance(other, Template):
            other = other.new()
        if isinstance(other, fs.Fill):
            return self.new() + other
        return NotImplemented


class Systematic(Template):
    def __init__(self, name: str, systs: Iterable[LabelLike], *axes: AxesMixin | tuple, weight: FieldLike = 'weight', **fill_args: FieldLike):
        super().__init__((name, ''), (), **fill_args)
        weight = astuple(weight)
        if len(axes) == 0:
            axes = Collection.current.duplicate_axes(name)
        for _var in systs:
            _var = Label(_var)
            self._name.display = f'({_var.display})'
            self._add(_var.code, *axes, weight=weight +
                      _default_field(_var.code))
        self._name.display = ''
