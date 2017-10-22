
from .client import Client


VERSION = (0, 0, 8)
__version__ = '.'.join(str(x) for x in VERSION)

__all__ = ['Client']
