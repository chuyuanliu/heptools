from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Optional

from ._parser import ConfigParser

if TYPE_CHECKING:
    from ._protocol import Configurable


class _unset: ...


class _status:
    updated: dict[str, Any] = {}
    default: dict[str, Any] = {}
    frozen: bool = False

    @classmethod
    def get(cls, name: str):
        value = cls.updated.get(name, _unset)
        if value is _unset:
            try:
                value = cls.default[name]
            except KeyError:
                raise AttributeError(f"Config {name} is not set.")
        return value


@contextmanager
def _freeze(value: bool):
    cache = _status.frozen
    _status.frozen = value
    yield
    _status.frozen = cache


@contextmanager
def _override(path_or_dict: tuple, parser: ConfigParser):
    cache = _status.updated
    _status.updated = deepcopy(cache)
    parser(*path_or_dict, flat=True, result=_status.updated)
    yield
    _status.updated = cache


class _pickler:
    def __getstate__(self):
        return _status.updated

    def __setstate__(self, state):
        self._data = state

    def __call__(self):
        _status.updated.update(self._data)
        del self._data


class ConfigManager:
    __parser = ConfigParser()

    @classmethod
    def update(cls, *path_or_dict: str | dict, parser: ConfigParser = None):
        parser = (parser or cls.__parser)(
            *path_or_dict, flat=True, result=_status.updated
        )

    @classmethod
    def override(cls, *path_or_dict: str | dict, parser: ConfigParser = None):
        return _override(path_or_dict, parser=parser or cls.__parser)

    @staticmethod
    def freeze():
        return _freeze(True)

    @staticmethod
    def unfreeze():
        return _freeze(False)

    @staticmethod
    def take_snapshot():
        return deepcopy(_status.updated)

    @staticmethod
    def restore_snapshot(snapshot: dict[str, Any]):
        _status.updated = snapshot

    @staticmethod
    def initializer():
        return _pickler()

    @staticmethod
    def inspect(configurable: Optional[Configurable | type[Configurable]] = None):
        from ._protocol import Configurable

        mode = 0
        if configurable is None:
            return dict(sorted((_status.default | _status.updated).items()))
        if isinstance(configurable, Configurable):
            if (cache := configurable.__config_cache__) is not None:
                return dict(sorted(cache.items()))
            mode = 1
        elif isinstance(configurable, type) and issubclass(configurable, Configurable):
            mode = 1
        match mode:
            case 1:
                return {
                    k: _status.get(k) for k in sorted(configurable.__config_attrs__)
                }
        raise TypeError(
            f'Cannot inspect non-configurable "<{type(configurable).__name__}> {configurable}"'
        )
