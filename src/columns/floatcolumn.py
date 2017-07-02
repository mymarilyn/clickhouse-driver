import struct

from .base import Column


class FloatColumn(Column):
    py_types = (float, int)
    format = None
    inf = float('inf')

    def read(self, buf):
        return self._read(buf, self.format)

    def _read_null(self, buf):
        self.read(buf)

    def write(self, value, buf):
        try:
            x = struct.pack(self.format, value)
        except OverflowError:
            # Write +/- inf if float overflows
            # Client has it's behaviour now.
            value = self.inf if value > 0 else -self.inf
            x = struct.pack(self.format, value)

        buf.write(x)

    def _write_null(self, buf):
        self.write(0, buf)


class Float32(FloatColumn):
    ch_type = 'Float32'
    format = '<f'


class Float64(FloatColumn):
    ch_type = 'Float64'
    format = '<d'
