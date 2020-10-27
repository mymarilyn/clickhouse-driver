from functools import wraps
from unittest import SkipTest


def check_numpy(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except RuntimeError as e:
            if 'NumPy' in str(e):
                raise SkipTest('Numpy package is not installed')

    return wrapper
