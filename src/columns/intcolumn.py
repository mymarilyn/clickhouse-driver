import six

from .. import errors
from .base import Column, size_by_type


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
