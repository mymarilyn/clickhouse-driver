from os import getenv
from datetime import date, timedelta

from .base import FormatColumn


epoch_start = date(1970, 1, 1)
epoch_end = date(2149, 6, 6)

epoch_start_date32 = date(1900, 1, 1)
# Why was it 17 year earlier that limit set in clickhouse?
# epoch_end_date32 = date(2283, 11, 11)
epoch_end_date32 = date(2299, 12, 31)


class LazyLUT(dict):
    def __init__(self, *args, _default_factory, _start, _end, **kwargs):
        super().__init__(*args, **kwargs)
        self._default_factory = _default_factory
        # assuming start & env validations are already present in CH & before_write_items, may be redundant
        self._start = _start
        self._end = _end

    def __missing__(self, key):
        if not (self._start <= key <= self._end):
            raise KeyError(key)

        return self.setdefault(key, self._default_factory(key))


class DateColumn(FormatColumn):
    ch_type = 'Date'
    py_types = (date, )
    format = 'H'

    min_value = epoch_start
    max_value = epoch_end

    date_lut_days = (epoch_end - epoch_start).days + 1

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        date_lut_reverse = self.date_lut_reverse
        min_value = self.min_value
        max_value = self.max_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if item is not date:
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


if getenv('CLICKHOUSE_DRIVER_LASY_DATE_LUT'):
    DateColumn.date_lut = LazyLUT(
        _default_factory=lambda x: epoch_start + timedelta(x),
        _start=0,
        _end=DateColumn.date_lut_days - 1,
    )
    DateColumn.date_lut_reverse = LazyLUT(
        _default_factory= lambda x: (x - epoch_start).days,
        _start=DateColumn.min_value,
        _end=DateColumn.max_value,
    )

    Date32Column.date_lut = LazyLUT(
        _default_factory=lambda x: epoch_start + timedelta(x),
        _start=(epoch_start_date32 - epoch_start).days,
        _end=Date32Column.date_lut_days - 1,
    )
    Date32Column.date_lut_reverse = LazyLUT(
        _default_factory= lambda x: (x - epoch_start).days,
        _start=Date32Column.min_value,
        _end=Date32Column.max_value,
    )
else:
    DateColumn.date_lut = {x: epoch_start + timedelta(x) for x in range(DateColumn.date_lut_days)}
    DateColumn.date_lut_reverse = {value: key for key, value in DateColumn.date_lut.items()}

    Date32Column.date_lut = {
        x: epoch_start + timedelta(x)
        for x in range((epoch_start_date32 - epoch_start).days, Date32Column.date_lut_days)
    }
    Date32Column.date_lut_reverse = {value: key for key, value in Date32Column.date_lut.items()}
