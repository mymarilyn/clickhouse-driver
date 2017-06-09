import struct
from datetime import date, datetime
from calendar import timegm

from enum import Enum
import six

from .. import errors
from ..reader import read_binary_str, read_binary_str_fixed_len
from ..writer import write_binary_str, write_binary_str_fixed_len
from ..util.tzinfo import tzutc

if six.PY3:
    from queue import Queue
else:
    from Queue import Queue

size_by_type = {
    'Date': 2,
    'DateTime': 4,
    'Int8': 1,
    'UInt8': 1,
    'Int16': 2,
    'UInt16': 2,
    'Int32': 4,
    'UInt32': 4,
    'Int64': 8,
    'UInt64': 8,
    'Float32': 4,
    'Float64': 8,
    'Enum8': 1,
    'Enum16': 2
}


class Column(object):
    ch_type = None
    py_types = None

    @property
    def size(self):
        return size_by_type[self.ch_type]

    def read(self, buf):
        raise NotImplementedError

    def write(self, value, buf):
        raise NotImplementedError

    def _read(self, buf, fmt):
        return struct.unpack(fmt, buf.read(self.size))[0]

    def _write(self, x, buf, fmt):
        return buf.write(struct.pack(fmt, x))


class String(Column):
    ch_type = 'String'
    py_types = six.string_types

    def read(self, buf):
        return read_binary_str(buf)

    def write(self, value, buf):
        write_binary_str(value, buf)


class FixedString(Column):
    ch_type = 'FixedString'
    py_types = six.string_types

    def __init__(self, length):
        self.length = length
        super(FixedString, self).__init__()

    def read(self, buf):
        return read_binary_str_fixed_len(buf, self.length).strip('\x00')

    def write(self, value, buf):
        try:
            write_binary_str_fixed_len(value, buf, self.length)
        except ValueError:
            raise errors.TooLargeStringSize()


class DateColumn(Column):
    ch_type = 'Date'
    # TODO: string
    py_types = (date, )
    format = '<H'

    offset = 24 * 3600

    def read(self, buf):
        x = self._read(buf, self.format)
        return date.fromtimestamp(x * self.offset)

    def write(self, value, buf):
        x = timegm(value.timetuple()) // self.offset
        self._write(x, buf, self.format)


class DateTimeColumn(Column):
    ch_type = 'DateTime'
    # TODO: string
    py_types = (datetime, )
    format = '<I'

    utc = tzutc()

    def read(self, buf):
        x = self._read(buf, self.format)
        return datetime.fromtimestamp(x, tz=self.utc).replace(tzinfo=None)

    def write(self, value, buf):
        x = int(timegm(value.timetuple()))
        self._write(x, buf, self.format)


class IntColumn(Column):
    py_types = six.integer_types

    def __init__(self):
        super(IntColumn, self).__init__()

        self.mask = (1 << 8 * size_by_type[self.ch_type]) - 1

    # Chop only bytes that fit current type
    def _prepare(self, value):
        sign = 1 if value > 0 else -1
        return sign * (abs(value) & self.mask)

    def read(self, buf):
        return self._read(buf, self.format)

    def write(self, value, buf):
        self._write(self._prepare(value), buf, self.format)


class UIntColumn(IntColumn):
    def write(self, value, buf):
        if value < 0:
            raise errors.TypeMismatchError(
                'Type mismatch in VALUES section. '
                'Expected {} got {}'.format(self.ch_type, value)
            )

        super(UIntColumn, self).write(value, buf)


class Int8Column(IntColumn):
    ch_type = 'Int8'
    format = '<b'


class Int16Column(IntColumn):
    ch_type = 'Int16'
    format = '<h'


class Int32Column(IntColumn):
    ch_type = 'Int32'
    format = '<i'


class Int64Column(IntColumn):
    ch_type = 'Int64'
    format = '<q'


class UInt8Column(UIntColumn):
    ch_type = 'UInt8'
    format = '<B'


class UInt16Column(UIntColumn):
    ch_type = 'UInt16'
    format = '<H'


class UInt32Column(UIntColumn):
    ch_type = 'UInt32'
    format = '<I'


class UInt64Column(UIntColumn):
    ch_type = 'UInt64'
    format = '<Q'


class FloatColumn(Column):
    py_types = (float, int)
    format = None
    inf = float('inf')

    def read(self, buf):
        return self._read(buf, self.format)

    def write(self, value, buf):
        try:
            x = struct.pack(self.format, value)
        except OverflowError:
            # Write +/- inf if float overflows
            # Client has it's behaviour now.
            value = self.inf if value > 0 else -self.inf
            x = struct.pack(self.format, value)

        buf.write(x)


class Float32(FloatColumn):
    ch_type = 'Float32'
    format = '<f'


class Float64(FloatColumn):
    ch_type = 'Float64'
    format = '<d'


class EnumColumn(IntColumn):
    py_types = (Enum, ) + six.integer_types + six.string_types
    format = '<b'

    def __init__(self, enum_cls):
        self.enum_cls = enum_cls
        super(EnumColumn, self).__init__()

    def read(self, buf):
        value = super(EnumColumn, self).read(buf)
        return self.enum_cls(value).name

    def write(self, value, buf):
        source_value = value.name if isinstance(value, Enum) else value
        enum_cls = self.enum_cls

        # Check real enum value
        try:
            if isinstance(source_value, six.string_types):
                value = enum_cls[source_value].value
            else:
                value = enum_cls(source_value).value

        except (ValueError, KeyError):
            choices = ', '.join(
                "'{}' = {}".format(x.name, x.value) for x in enum_cls
            )
            enum_str = '{}({})'.format(enum_cls.__name__, choices)

            raise errors.LogicalError(
                "Unknown element '{}' for type {}"
                .format(source_value, enum_str)
            )

        super(EnumColumn, self).write(value, buf)


class Enum8Column(EnumColumn):
    ch_type = 'Enum8'
    format = '<b'


class Enum16Column(EnumColumn):
    ch_type = 'Enum16'
    format = '<h'


class ArrayColumn(Column):
    py_types = (list, tuple)

    def __init__(self, nested_column):
        self.size_column = UInt64Column()
        self.nested_column = nested_column
        super(ArrayColumn, self).__init__()

    def _write_sizes(self, value, buf):
        q = Queue()
        q.put((self, value, 0))

        cur_depth = 0
        offset = 0
        while not q.empty():
            column, value, depth = q.get_nowait()

            if cur_depth != depth:
                cur_depth = depth
                offset = 0

            offset += len(value)
            self.size_column.write(offset, buf)

            nested_column = column.nested_column
            if isinstance(nested_column, ArrayColumn):
                for x in value:
                    q.put((nested_column, x, cur_depth + 1))

    def _write_data(self, value, buf):
        if isinstance(self.nested_column, ArrayColumn):
            for x in value:
                self.nested_column._write_data(x, buf)
        else:
            for x in value:
                self.nested_column.write(x, buf)

    def write(self, value, buf):
        self._write_sizes(value, buf)
        self._write_data(value, buf)

    def _read_data(self, size, buf):
        q = Queue()
        q.put((self, size, 0))

        data = []
        slices_series = []

        cur_depth = 0
        prev_offset = 0
        slices = []
        # Read and store info about slices.
        while not q.empty():
            column, size, depth = q.get_nowait()

            if cur_depth != depth:
                cur_depth = depth
                prev_offset = 0
                slices_series.append(slices)
                slices = []

            nested_column = column.nested_column
            if isinstance(nested_column, ArrayColumn):
                for _i in range(size):
                    offset = self.size_column.read(buf)
                    q.put((nested_column, offset - prev_offset, cur_depth + 1))
                    slices.append((prev_offset, offset))
                    prev_offset = offset

            # Read data
            else:
                data.extend(nested_column.read(buf) for _i in range(size))

        # Build nested tuple structure.
        for slices in reversed(slices_series):
            nested_data = []
            for slice_from, slice_to in slices:
                nested_data.append(tuple(data[slice_from:slice_to]))
            data = nested_data

        return tuple(data)

    def read(self, buf):
        size = self.size_column.read(buf)
        return self._read_data(size, buf)


all_columns = [
    DateColumn, DateTimeColumn, String, Float32, Float64,
    Int8Column, Int16Column, Int32Column, Int64Column,
    UInt8Column, UInt16Column, UInt32Column, UInt64Column
]

column_by_type = {c.ch_type: c for c in all_columns}


def create_enum_column(spec):
    if spec.startswith('Enum8'):
        params = spec[6:-1]
        cls = Enum8Column
    else:
        params = spec[7:-1]
        cls = Enum16Column

    d = {}
    for param in params.split(", '"):
        pos = param.rfind("'")
        name = param[:pos].lstrip("'")
        value = int(param[pos + 1:].lstrip(' ='))
        d[name] = value

    return cls(Enum(cls.ch_type, d))


def create_array_column(spec):
    inner = spec[6:-1]
    return ArrayColumn(get_column_by_spec(inner))


def get_column_by_spec(spec):
    if spec.startswith('FixedString'):
        length = int(spec[12:-1])
        return FixedString(length)

    elif spec.startswith('Enum'):
        return create_enum_column(spec)

    elif spec.startswith('Array'):
        return create_array_column(spec)

    else:
        cls = column_by_type[spec]
        return cls()
