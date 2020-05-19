import sys


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


if PY3:
    import io
    StringIO = io.StringIO

else:
    import StringIO
    StringIO = StringIO.StringIO
