from calendar import timegm
from datetime import datetime
from time import mktime

from pytz import timezone as get_timezone, utc

from .base import FormatColumn


class DateTimeColumn(FormatColumn):
    ch_type = 'DateTime'
    py_types = (datetime, int)
    format = 'I'

    def __init__(self, timezone=None, offset_naive=True, **kwargs):
        self.timezone = timezone
        self.offset_naive = offset_naive
        super(DateTimeColumn, self).__init__(**kwargs)

    def after_read_items(self, items, nulls_map=None):
        timezone = self.timezone
        fromtimestamp = datetime.fromtimestamp

        if self.offset_naive:
            if nulls_map is None:
                return tuple(
                    fromtimestamp(item, timezone).replace(tzinfo=None)
                    for item in items
                )
            else:
                return tuple(
                    (None if is_null else
                     fromtimestamp(items[i], timezone).replace(tzinfo=None))
                    for i, is_null in enumerate(nulls_map)
                )
        else:
            if nulls_map is None:
                return tuple(fromtimestamp(item, timezone) for item in items)
            else:
                return tuple(
                    (None if is_null else fromtimestamp(items[i], timezone))
                    for i, is_null in enumerate(nulls_map)
                )

    def before_write_item(self, value):
        if isinstance(value, int):
            # support supplying raw integers to avoid
            # costly timezone conversions when using datetime
            return value

        if self.timezone:
            # Set server's timezone for offset-naive datetime.
            if value.tzinfo is None:
                value = self.timezone.localize(value)

            value = value.astimezone(utc)
            return int(timegm(value.timetuple()))

        else:
            # If datetime is offset-aware use it's timezone.
            if value.tzinfo is not None:
                value = value.astimezone(utc)
                return int(timegm(value.timetuple()))

            return int(mktime(value.timetuple()))


def create_datetime_column(spec, column_options):
    context = column_options['context']

    tz_name = timezone = None
    offset_naive = True

    # Use column's timezone if it's specified.
    if spec[-1] == ')':
        tz_name = spec[10:-2]
        offset_naive = False
    else:
        if not context.settings.get('use_client_time_zone', False):
            tz_name = context.server_info.timezone

    if tz_name:
        timezone = get_timezone(tz_name)

    return DateTimeColumn(
        timezone=timezone, offset_naive=offset_naive, **column_options
    )
