def ensure(__str: str, __prefix: str = None, __suffix: str = None):
    if __prefix is not None and not __str.startswith(__prefix):
        __str = __prefix + __str
    if __suffix is not None and not __str.endswith(__suffix):
        __str = __str + __suffix
    return __str
