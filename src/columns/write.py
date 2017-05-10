from .column import column_by_type
from ..errors import TypeMismatchError


def write_column(ch_type, data, buf):
    column = column_by_type[ch_type]
    py_types = column.py_types
    write = column.write

    for x in data:
        if not isinstance(x, py_types):
            raise TypeMismatchError(
                'Type mismatch in VALUES section. '
                'Expected {} got {}'.format(ch_type, type(x))
            )

        write(x, buf)
