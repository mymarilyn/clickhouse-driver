from .column import get_column_by_spec
from ..errors import TypeMismatchError


def write_column(column_spec, data, buf):
    column = get_column_by_spec(column_spec)
    py_types = column.py_types
    write = column.write

    for x in data:
        if not isinstance(x, py_types):
            raise TypeMismatchError(
                'Type mismatch in VALUES section. '
                'Expected {} got {}'.format(column_spec, type(x))
            )

        write(x, buf)
