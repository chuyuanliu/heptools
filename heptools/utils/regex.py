import re
from typing import Callable, Iterable

from ..typetools import check_type

CompiledPattern = re.Pattern | bool
MultiPattern = Iterable[str] | CompiledPattern


def match_single(pattern: CompiledPattern, string: str) -> bool:
    if isinstance(pattern, re.Pattern):
        return pattern.match(string) is not None
    else:
        return pattern


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
