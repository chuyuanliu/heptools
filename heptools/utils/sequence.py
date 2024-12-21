from dataclasses import dataclass
from typing import Annotated, Generic, TypeVar

_ItemT = TypeVar("_ItemT")
SequenceIndex = Annotated[int, ">=0"]


@dataclass
class ConstantSequence(Generic[_ItemT]):
    value: _ItemT

    def __getitem__(self, _) -> _ItemT:
        return self.value


@dataclass
class BoundedGeometricSequence(Generic[_ItemT]):
    initial: _ItemT
    ratio: _ItemT
    bound: SequenceIndex

    def __getitem__(self, index: SequenceIndex) -> _ItemT:
        return self.initial * (self.ratio ** min(index, self.bound))


@dataclass
class BoundedArithmeticSequence(Generic[_ItemT]):
    initial: _ItemT
    difference: _ItemT
    bound: SequenceIndex

    def __getitem__(self, index: SequenceIndex) -> _ItemT:
        return self.initial + self.difference * min(index, self.bound)
