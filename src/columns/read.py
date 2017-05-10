from .column import column_by_type


def read_column(ch_type, rows, buf):
    read = column_by_type[ch_type].read
    return tuple(read(buf) for _i in range(rows))
