import struct
from datetime import date, datetime
from calendar import timegm

import six

from .. import errors
from ..reader import read_binary_str
from ..writer import write_binary_str
from ..util.tzinfo import tzutc


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
    'Float64': 8
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


all_columns = [
    DateColumn(), DateTimeColumn(), String(), Float32(), Float64(),
    Int8Column(), Int16Column(), Int32Column(), Int64Column(),
    UInt8Column(), UInt16Column(), UInt32Column(), UInt64Column(),
]

column_by_type = {c.ch_type: c for c in all_columns}
