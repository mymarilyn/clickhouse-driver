from .column import get_column_by_spec


def read_column(column_spec, rows, buf):
    column = get_column_by_spec(column_spec)
    read = column.read
    return tuple(read(buf) for _i in range(rows))
