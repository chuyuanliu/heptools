import re
from typing import Callable, Iterable, Literal

from ..typetools import check_type

CompiledPattern = re.Pattern | bool
MultiPattern = Iterable[str] | CompiledPattern


def match_single(pattern: CompiledPattern, string: str) -> bool:
    if isinstance(pattern, re.Pattern):
        return pattern.match(string) is not None
    else:
        return pattern


def filter_unique(pattern: CompiledPattern, *strings: str) -> set[str]:
    if isinstance(pattern, re.Pattern):
        return {*filter(pattern.match, strings)}
    else:
        if pattern:
            return {*strings}
        else:
            return set()


class compiler:
    def __init__(self, func: Callable[[Iterable[str]], str]):
        self._func = func

    def __call__(self, patterns: MultiPattern) -> CompiledPattern:
        if check_type(patterns, re.Pattern):
            return patterns
        elif check_type(patterns, Iterable[str]):
            return re.compile(self._func(patterns))
        else:
            return bool(patterns)


@compiler
def compile_any_wholeword(patterns: MultiPattern):
    return f'^({"|".join(patterns)})$'


class SelectSkip:
    def __init__(
        self,
        select: MultiPattern = True,
        skip: MultiPattern = False,
        logic: Literal['and', 'or'] = 'and',
        method: Callable[[MultiPattern],
                         CompiledPattern] = compile_any_wholeword,
    ):
        self._select = method(select)
        self._skip = method(skip)
        self._logic = logic

    def __call__(self, *strings: str) -> set[str]:
        select = filter_unique(self._select, *strings)
        skip = filter_unique(self._skip, *strings)
        if self._logic == 'or':
            return {*strings} - (skip - select)
        elif self._logic == 'and':
            return select - skip
