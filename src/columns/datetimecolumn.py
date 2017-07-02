from calendar import timegm
from datetime import datetime

from ..util.tzinfo import tzutc
from .base import FormatColumn


class DateTimeColumn(FormatColumn):
    ch_type = 'DateTime'
    # TODO: string
    py_types = (datetime, )
    format = '<I'

    utc = tzutc()

    def read(self, buf):
        x = self._read(buf)
        return datetime.fromtimestamp(x, tz=self.utc).replace(tzinfo=None)

    def write(self, value, buf):
        x = int(timegm(value.timetuple()))
        self._write(x, buf)
