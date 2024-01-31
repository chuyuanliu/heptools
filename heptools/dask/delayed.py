from functools import wraps

from ..utils.wrapper import OptionalDecorator


class _Delayed(OptionalDecorator):
    @property
    def _switch(self):
        return 'dask'

    def _decorate(self, __func):
        from dask import delayed
        return delayed(__func)


def delayed(__func):
    return wraps(__func)(_Delayed(__func))
