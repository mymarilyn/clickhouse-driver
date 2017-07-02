from calendar import timegm
from datetime import datetime

from ..util.tzinfo import tzutc
from .base import Column


class DateTimeColumn(Column):
    ch_type = 'DateTime'
    # TODO: string
    py_types = (datetime, )
    format = '<I'

    utc = tzutc()

    def read(self, buf):
        x = self._read(buf, self.format)
        return datetime.fromtimestamp(x, tz=self.utc).replace(tzinfo=None)

    def _read_null(self, buf):
        self._read(buf, self.format)

    def write(self, value, buf):
        x = int(timegm(value.timetuple()))
        self._write(x, buf, self.format)

    def _write_null(self, buf):
        self._write(0, buf, self.format)
