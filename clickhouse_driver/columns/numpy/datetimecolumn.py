try:
    import numpy as np
except ImportError:
    numpy = None

try:
    import pandas as pd
except ImportError:
    pandas = None

from pytz import timezone as get_timezone
from tzlocal import get_localzone

from .base import NumpyColumn


class NumpyDateTimeColumn(NumpyColumn):
    dtype = np.dtype(np.uint32)

    def __init__(self, timezone=None, offset_naive=True, local_timezone=None,
                 **kwargs):
        self.timezone = timezone
        self.offset_naive = offset_naive
        self.local_timezone = local_timezone
        super(NumpyDateTimeColumn, self).__init__(**kwargs)

    def apply_timezones(self, dt):
        ts = pd.to_datetime(dt, utc=True)
        timezone = self.timezone if self.timezone else self.local_timezone

        ts = ts.tz_convert(timezone)
        if self.offset_naive:
            ts = ts.tz_localize(None)

        return ts.to_numpy()

    def read_items(self, n_items, buf):
        data = super(NumpyDateTimeColumn, self).read_items(n_items, buf)
        dt = data.astype('datetime64[s]')
        return self.apply_timezones(dt)


class NumpyDateTime64Column(NumpyDateTimeColumn):
    dtype = np.dtype(np.uint64)

    max_scale = 6

    def __init__(self, scale=0, **kwargs):
        self.scale = scale
        super(NumpyDateTime64Column, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        scale = 10 ** self.scale
        frac_scale = 10 ** (self.max_scale - self.scale)

        data = super(NumpyDateTimeColumn, self).read_items(n_items, buf)
        seconds = (data // scale).astype('datetime64[s]')
        microseconds = ((data % scale) * frac_scale).astype('timedelta64[us]')

        dt = seconds + microseconds
        return self.apply_timezones(dt)


def create_numpy_datetime_column(spec, column_options):
    if spec.startswith('DateTime64'):
        cls = NumpyDateTime64Column
        spec = spec[11:-1]
        params = spec.split(',', 1)
        column_options['scale'] = int(params[0])
        if len(params) > 1:
            spec = params[1].strip() + ')'
    else:
        cls = NumpyDateTimeColumn
        spec = spec[9:]

    context = column_options['context']

    tz_name = timezone = None
    offset_naive = True
    local_timezone = None

    # As Numpy do not use local timezone for converting timestamp to
    # datetime we need always detect local timezone for manual converting.
    try:
        local_timezone = get_localzone().zone
    except Exception:
        pass

    # Use column's timezone if it's specified.
    if spec and spec[-1] == ')':
        tz_name = spec[1:-2]
        offset_naive = False
    else:
        if not context.settings.get('use_client_time_zone', False):
            if local_timezone != context.server_info.timezone:
                tz_name = context.server_info.timezone

    if tz_name:
        timezone = get_timezone(tz_name)

    return cls(timezone=timezone, offset_naive=offset_naive,
               local_timezone=local_timezone, **column_options)
