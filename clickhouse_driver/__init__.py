
from .client import Client
from .dbapi import connect
from .util.escape import escape_params


VERSION = (0, 2, 4)
__version__ = '.'.join(str(x) for x in VERSION)

__all__ = ['Client', 'connect']
