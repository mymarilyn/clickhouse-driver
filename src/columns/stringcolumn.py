
from .. import errors
from ..reader import read_binary_bytes, read_binary_bytes_fixed_len
from ..writer import write_binary_bytes, write_binary_bytes_fixed_len
from ..util import compat
from .base import CustomItemColumn


class String(CustomItemColumn):
    ch_type = 'String'
    py_types = compat.string_types

    # TODO: pass user encoding here

    def try_encode(self, value):
        if not isinstance(value, bytes):
            return value.encode('utf-8')
        return value

    def try_decode(self, value):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            # Do nothing. Just return bytes.
            pass

        return value

    def read(self, buf):
        return self.try_decode(read_binary_bytes(buf))

    def _read_null(self, buf):
        self.read(buf)

    def write(self, value, buf):
        write_binary_bytes(self.try_encode(value), buf)

    def _write_null(self, buf):
        self.write('', buf)


class FixedString(String):
    ch_type = 'FixedString'

    def __init__(self, length, **kwargs):
        self.length = length
        super(FixedString, self).__init__(**kwargs)

    def read(self, buf):
        text = self.try_decode(read_binary_bytes_fixed_len(buf, self.length))
        if isinstance(text, bytes):
            strip = b'\x00'
        else:
            strip = '\x00'
        return text.strip(strip)

    def write(self, value, buf):
        try:
            value = self.try_encode(value)
            write_binary_bytes_fixed_len(value, buf, self.length)
        except ValueError:
            raise errors.TooLargeStringSize()


def create_fixed_string_column(spec):
    length = int(spec[12:-1])
    return FixedString(length)
