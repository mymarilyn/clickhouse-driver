from calendar import timegm
from datetime import date

from .base import FormatColumn


class DateColumn(FormatColumn):
    ch_type = 'Date'
    py_types = (date, )
    format = 'H'

    offset = 24 * 3600

    def before_write_item(self, value):
        return timegm(value.timetuple()) // self.offset

    def after_read_item(self, value):
        return date.fromtimestamp(value * self.offset)
