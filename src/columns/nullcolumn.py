from .intcolumn import FormatColumn


class NullColumn(FormatColumn):
    ch_type = 'Null'
    format = '<b'

    @property
    def size(self):
        return 1

    def write(self, value, buf):
        self._write(1, buf)

    def read(self, buf):
        self._read(buf)
