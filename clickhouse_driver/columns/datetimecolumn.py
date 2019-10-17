from calendar import timegm
from datetime import datetime
from time import mktime

from pytz import timezone as get_timezone, utc
from tzlocal import get_localzone

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
        tz = self.timezone
        fromtimestamp = datetime.fromtimestamp

        # A bit ugly copy-paste. But it helps save time on items
        # processing by avoiding lambda calls or if in loop.
        if self.offset_naive:
            if tz:
                if nulls_map is None:
                    return tuple(
                        fromtimestamp(item, tz).replace(tzinfo=None)
                        for item in items
                    )
                else:
                    return tuple(
                        (None if is_null else
                         fromtimestamp(items[i], tz).replace(tzinfo=None))
                        for i, is_null in enumerate(nulls_map)
                    )
            else:
                if nulls_map is None:
                    return tuple(fromtimestamp(item) for item in items)
                else:
                    return tuple(
                        (None if is_null else fromtimestamp(items[i]))
                        for i, is_null in enumerate(nulls_map)
                    )

        else:
            if nulls_map is None:
                return tuple(fromtimestamp(item, tz) for item in items)
            else:
                return tuple(
                    (None if is_null else fromtimestamp(items[i], tz))
                    for i, is_null in enumerate(nulls_map)
                )

    def before_write_items(self, items, nulls_map=None):
        timezone = self.timezone
        null_value = self.null_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if isinstance(item, int):
                # support supplying raw integers to avoid
                # costly timezone conversions when using datetime
                continue

            if timezone:
                # Set server's timezone for offset-naive datetime.
                if item.tzinfo is None:
                    item = timezone.localize(item)

                item = item.astimezone(utc)
                items[i] = int(timegm(item.timetuple()))

            else:
                # If datetime is offset-aware use it's timezone.
                if item.tzinfo is not None:
                    item = item.astimezone(utc)
                    items[i] = int(timegm(item.timetuple()))

                else:
                    items[i] = int(mktime(item.timetuple()))


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
            try:
                local_timezone = get_localzone().zone
            except Exception:
                local_timezone = None

            if local_timezone != context.server_info.timezone:
                tz_name = context.server_info.timezone

    if tz_name:
        timezone = get_timezone(tz_name)

    return DateTimeColumn(
        timezone=timezone, offset_naive=offset_naive, **column_options
    )
