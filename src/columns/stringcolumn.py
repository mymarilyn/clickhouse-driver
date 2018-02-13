
from .. import errors
from ..reader import read_bytes, read_bytes_fixed_len
from ..writer import write_bytes, write_bytes_fixed_len
from ..util import compat
from .base import CustomItemColumn


class String(CustomItemColumn):
    ch_type = 'String'
    py_types = compat.string_types

    # TODO: pass user encoding here

    def read(self, buf):
        return read_bytes(buf)

    def _read_null(self, buf):
        self.read(buf)

    def write(self, value, buf):
        write_bytes(value, buf)

    def _write_null(self, buf):
        self.write('', buf)


class FixedString(String):
    ch_type = 'FixedString'

    def __init__(self, length, **kwargs):
        self.length = length
        super(FixedString, self).__init__(**kwargs)

    def read(self, buf):
        text = read_bytes_fixed_len(buf, self.length)
        if isinstance(text, bytes):
            strip = b'\x00'
        else:
            strip = '\x00'
        return text.strip(strip)

    def write(self, value, buf):
        try:
            write_bytes_fixed_len(value, buf, self.length)
        except ValueError:
            raise errors.TooLargeStringSize()
