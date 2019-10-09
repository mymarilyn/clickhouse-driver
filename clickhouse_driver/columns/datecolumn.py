from datetime import date, timedelta

from .base import FormatColumn


epoch_start = date(1970, 1, 1)


class DateColumn(FormatColumn):
    ch_type = 'Date'
    py_types = (date, )
    format = 'H'

    epoch_start = epoch_start
    epoch_end = date(2105, 12, 31)

    date_lut = {x: epoch_start + timedelta(x) for x in range(65535)}
    date_lut_reverse = {value: key for key, value in date_lut.items()}

    def before_write_item(self, value):
        if type(value) != date:
            value = date(value.year, value.month, value.day)

        if value > self.epoch_end or value < self.epoch_start:
            return 0
        return self.date_lut_reverse[value]

    def after_read_items(self, items, nulls_map=None):
        date_lut = self.date_lut

        if nulls_map is None:
            return tuple(date_lut[item] for item in items)
        else:
            return tuple(
                (None if is_null else date_lut[items[i]])
                for i, is_null in enumerate(nulls_map)
            )
