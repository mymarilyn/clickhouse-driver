from .. import errors
from .arraycolumn import create_array_column
from .datecolumn import DateColumn
from .datetimecolumn import DateTimeColumn
from .enumcolumn import create_enum_column
from .floatcolumn import Float32, Float64
from .intcolumn import (
    Int8Column, Int16Column, Int32Column, Int64Column,
    UInt8Column, UInt16Column, UInt32Column, UInt64Column
)
from .nullcolumn import NullColumn
from .nullablecolumn import create_nullable_column
from .stringcolumn import String, FixedString


column_by_type = {c.ch_type: c for c in [
    DateColumn, DateTimeColumn, String, Float32, Float64,
    Int8Column, Int16Column, Int32Column, Int64Column,
    UInt8Column, UInt16Column, UInt32Column, UInt64Column,
    NullColumn
]}


def get_column_by_spec(spec):
    if spec.startswith('FixedString'):
        length = int(spec[12:-1])
        return FixedString(length)

    elif spec.startswith('Enum'):
        return create_enum_column(spec)

    elif spec.startswith('Array'):
        return create_array_column(spec, get_column_by_spec)

    elif spec.startswith('Nullable'):
        return create_nullable_column(spec, get_column_by_spec)

    else:
        try:
            cls = column_by_type[spec]
            return cls()

        except KeyError as e:
            raise errors.UnknownTypeError('Unknown type {}'.format(e.args[0]))


def read_column(column_spec, rows, buf):
    column = get_column_by_spec(column_spec)
    return column.read_data(rows, buf)


def write_column(column_spec, data, buf):
    column = get_column_by_spec(column_spec)

    try:
        column.write_data(data, buf)

    # TODO: Raise different error. This error abuses python native.
    except TypeError as e:
        raise errors.TypeMismatchError(
            'Type mismatch in VALUES section. '
            'Expected {} got {}'.format(column_spec, type(e.args[0]))
        )
