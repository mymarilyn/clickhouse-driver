from datetime import date, timedelta

from .base import FormatColumn


epoch_start = date(1970, 1, 1)
epoch_end = date(2149, 6, 6)

epoch_start_date32 = date(1925, 1, 1)
epoch_end_date32 = date(2283, 11, 11)


class DateColumn(FormatColumn):
    ch_type = 'Date'
    py_types = (date, )
    format = 'H'

    min_value = epoch_start
    max_value = epoch_end

    date_lut_days = (epoch_end - epoch_start).days + 1
    date_lut = {x: epoch_start + timedelta(x) for x in range(date_lut_days)}
    date_lut_reverse = {value: key for key, value in date_lut.items()}

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        date_lut_reverse = self.date_lut_reverse
        min_value = self.min_value
        max_value = self.max_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if type(item) != date:
                item = date(item.year, item.month, item.day)

            if min_value <= item <= max_value:
                items[i] = date_lut_reverse[item]
            else:
                items[i] = 0

    def after_read_items(self, items, nulls_map=None):
        date_lut = self.date_lut

        if nulls_map is None:
            return tuple(date_lut[item] for item in items)
        else:
            return tuple(
                (None if is_null else date_lut[items[i]])
                for i, is_null in enumerate(nulls_map)
            )


class Date32Column(DateColumn):
    ch_type = 'Date32'
    format = 'i'

    min_value = epoch_start_date32
    max_value = epoch_end_date32

    date_lut_days = (epoch_end_date32 - epoch_start).days + 1
    date_lut = {
        x: epoch_start + timedelta(x)
        for x in range((epoch_start_date32 - epoch_start).days, date_lut_days)
    }
    date_lut_reverse = {value: key for key, value in date_lut.items()}
