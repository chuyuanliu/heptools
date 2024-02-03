from __future__ import annotations

from copy import deepcopy
from typing import Callable, Iterable, overload

import awkward as ak
import numpy as np
from hist import Hist
from hist.axis import (AxesMixin, Boolean, IntCategory, Integer, Regular,
                       StrCategory, Variable)

from ..aktools import (AnyArray, AnyInt, FieldLike, RealNumber, and_fields,
                       get_field, has_record, set_field)
from ..typetools import check_type
from . import template as _t


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


LazyFill = FieldLike | Callable
FillLike = LazyFill | AnyArray | RealNumber | bool
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


class FillError(Exception):
    __module__ = Exception.__module__


class HistError(Exception):
    __module__ = Exception.__module__


class Fill:
    threads = 1

    def __init__(self, fills: dict[str, list[str]] = None, weight='weight', **fill_args: FillLike):
        self._fills = {} if fills is None else fills
        self._kwargs = fill_args | {'weight': weight}

    def __add__(self, other: Fill | _t.Template) -> Fill:
        if isinstance(other, Fill):
            fills = other._fills | self._fills
            kwargs = other._kwargs | self._kwargs
            return self.__class__(fills, **kwargs)
        elif isinstance(other, _t.Template):
            return self + other.new()
        return NotImplemented

    def __call__(self, events: ak.Array, hists: Collection = ..., **fill_args: FillLike):
        self.fill(events, hists, **fill_args)

    def __setitem__(self, key: str, value: FillLike):
        self._kwargs[key] = value

    def __getitem__(self, key: str):
        return self._kwargs[key]

    def cache(self, events: ak.Array):
        if Collection.current is None:
            raise FillError('no histogram collection is specified')
        for k, v in self._kwargs.items():
            if (isinstance(v, str)
                    and isinstance(Collection.current._axes.get(k), StrCategory)):
                continue
            if check_type(v, FieldLike) and has_record(events, v) != v:
                set_field(events, v, get_field(events, v))

    def fill(self, events: ak.Array, hists: Collection = ..., **fill_args: FillLike):
        if hists is ...:
            if Collection.current is None:
                raise FillError('no histogram collection is specified')
            hists = Collection.current
        fill_args = self._kwargs | fill_args
        mask_categories = []
        for category in hists._categories:
            if category not in fill_args:
                if isinstance(hists._axes[category], StrCategory):
                    mask_categories.append(category)
                else:
                    fill_args[category] = _default_field(category)
        for category_args in hists._generate_category_combinations(mask_categories):
            mask = and_fields(events, *category_args.items())
            masked = events if mask is None else events[mask]
            if len(masked) == 0:
                continue
            for k, v in fill_args.items():
                if (isinstance(v, str) and k in hists._categories) or isinstance(v, (bool, RealNumber)):
                    category_args[k] = v
                elif check_type(v, FieldLike):
                    category_args[k] = get_field(masked, v)
                elif check_type(v, AnyArray):
                    category_args[k] = v if mask is None else v[mask]
                elif check_type(v, Callable):
                    category_args[k] = v(masked)
                else:
                    raise FillError(f'cannot fill "{k}" with "{v}"')
            jagged_args = {}
            counts_args = []
            for k, v in category_args.items():
                if isinstance(v, ak.Array):
                    try:
                        category_args[k] = ak.flatten(v)
                        count = ak.num(v)
                        for i, c in enumerate(counts_args):
                            if ak.all(c == count):
                                jagged_args[k] = i
                        if k not in jagged_args:
                            jagged_args[k] = len(counts_args)
                            counts_args.append(count)
                    except:
                        continue
            for name in self._fills:
                fills = {
                    k: f'{name}:{k}' if f'{name}:{k}' in category_args else k for k in self._fills[name]}
                shape = {jagged_args[k]
                         for k in fills.values() if k in jagged_args}
                if len(shape) == 0:
                    shape = None
                elif len(shape) == 1:
                    shape = counts_args[next(iter(shape))]
                else:
                    raise FillError(
                        f'cannot fill hist "{name}" with unmatched jagged arrays {jagged_args}')
                hist_args = {}
                for k, v in fills.items():
                    fill = category_args[v]
                    if shape is not None:
                        if v not in jagged_args and check_type(fill, AnyArray):
                            fill = np.repeat(fill, shape)
                    hist_args[k] = fill
                # https://github.com/scikit-hep/boost-histogram/issues/452 #
                if all([check_type(axis, StrCategory) for axis in hists._hists[name].axes]):
                    try:
                        weight = hist_args['weight']
                        if len(weight) > 0:
                            broadcasted = False
                            tobroadcast = None
                            for k, v in hist_args.items():
                                if k != 'weight':
                                    if check_type(v, AnyArray) and len(v) == len(weight):
                                        broadcasted = True
                                        break
                                    else:
                                        tobroadcast = k
                            if not broadcasted and tobroadcast is not None:
                                hist_args[tobroadcast] = np.full(
                                    len(weight), hist_args[tobroadcast])
                    except:
                        continue
                ############################################################
                hists._hists[name].fill(**hist_args, threads=self.threads)


class Collection:
    current: Collection = None
    _backend_fill = Fill
    _backend_hist = Hist

    def __init__(self, **categories):
        self._fills: dict[str, list[str]] = {}
        self._hists: dict[str, Hist] = {}
        self._categories = deepcopy(categories)
        self._axes:  dict[str, AxesMixin] = {k: _create_axis(
            (*(v if isinstance(v, tuple) else (v,)), k)) for k, v in self._categories.items()}
        self.cd()

    def add(self, name: str, *axes: AxisLike, **fill_args: FillLike):
        axes = [_create_axis(axis) for axis in axes]
        self._fills[name] = [_axis.name for _axis in axes]
        self._hists[name] = self._backend_hist(*self._axes.values(),
                                               *axes, storage='weight', label='Events')
        return self.auto_fill(name, **fill_args)

    def _generate_category_combinations(self, categories: list[str]) -> list[dict[str, str]]:
        if len(categories) == 0:
            return [{}]
        else:
            combs = np.stack(np.meshgrid(
                *(self._categories[category] for category in categories)), axis=-1).reshape((-1, len(categories)))
            return [dict(zip(categories, comb)) for comb in combs]

    def auto_fill(self, name: str, **fill_args: FillLike):
        default_args = {k: _default_field(
            k) for k in self._fills[name] if k not in fill_args}
        fill_args = {f'{name}:{k}': v for k,
                     v in fill_args.items()} | default_args
        fills = {name: self._fills[name] + [*self._categories] + ['weight']}
        return self._backend_fill(fills, **fill_args)

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
