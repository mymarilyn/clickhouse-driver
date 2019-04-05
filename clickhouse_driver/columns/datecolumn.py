from datetime import date, timedelta

from .base import FormatColumn


class DateColumn(FormatColumn):
    ch_type = 'Date'
    py_types = (date, )
    format = 'H'

    epoch_start = date(1970, 1, 1)
    epoch_end = date(2105, 1, 1)

    def before_write_item(self, value):
        if type(value) != date:
            value = date(value.year, value.month, value.day)

        diff = (value - self.epoch_start).days
        if value > self.epoch_end or diff < 0:
            return 0
        return diff

    def after_read_item(self, value):
        return self.epoch_start + timedelta(value)
