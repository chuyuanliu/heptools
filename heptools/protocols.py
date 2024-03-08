__all__ = ["alias"]


def alias(*methods: str):
    def wrapper(cls):
        for method in methods:
            if not hasattr(cls, method):
                raise TypeError(f"`{cls.__name__}.{method}()` is not defined")
            setattr(cls, f"__{method}__", getattr(cls, method))
        return cls

    return wrapper
