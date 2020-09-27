
from .client import Client
from .dbapi import connect


VERSION = (0, 1, 6)
__version__ = '.'.join(str(x) for x in VERSION)

__all__ = ['Client', 'connect']
