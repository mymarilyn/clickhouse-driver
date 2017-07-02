from calendar import timegm
from datetime import date

from .base import FormatColumn


class DateColumn(FormatColumn):
    ch_type = 'Date'
    # TODO: string
    py_types = (date, )
    format = '<H'

    offset = 24 * 3600

    def read(self, buf):
        x = self._read(buf)
        return date.fromtimestamp(x * self.offset)

    def write(self, value, buf):
        x = timegm(value.timetuple()) // self.offset
        self._write(x, buf)
