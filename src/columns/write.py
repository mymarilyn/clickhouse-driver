from .column import get_column_by_spec
from ..errors import TypeMismatchError


def write_column(column_spec, data, buf):
    column = get_column_by_spec(column_spec)

    try:
        column.write_data(data, buf)

    except TypeError as e:
        raise TypeMismatchError(
            'Type mismatch in VALUES section. '
            'Expected {} got {}'.format(column_spec, type(e.args[0]))
        )
