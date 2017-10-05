from calendar import timegm
from datetime import datetime

from ..util.tzinfo import tzutc
from .base import FormatColumn


class DateTimeColumn(FormatColumn):
    ch_type = 'DateTime'
    py_types = (datetime, )
    format = 'I'

    utc = tzutc()

    def after_read_item(self, value):
        return datetime.fromtimestamp(value, tz=self.utc).replace(tzinfo=None)

    def before_write_item(self, value):
        return int(timegm(value.timetuple()))
