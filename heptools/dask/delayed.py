class delayed:
    def __init__(self, func):
        self._func = func
        self._delayed = None

    def __call__(self, *args, dask: bool = False, **kwargs):
        if dask:
            if self._delayed is None:
                from dask import delayed
                self._delayed = delayed(self._func)
            return self._delayed(*args, **kwargs, dask=dask)
        else:
            return self._func(*args, **kwargs, dask=dask)
