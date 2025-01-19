from datetime import date, datetime, time
from enum import Enum
from functools import wraps, partial
from uuid import UUID

from pytz import timezone


escape_chars_map = {
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\0": "\\0",
    "\a": "\\a",
    "\v": "\\v",
    "\\": "\\\\",
    "'": "\\'"
}


def escape_datetime(item, context):
    server_tz = timezone(context.server_info.get_timezone())

    if item.tzinfo is not None:
        item = item.astimezone(server_tz)

    if item.microsecond:
        format = '%Y-%m-%d %H:%M:%S.%f'
    else:
        format = '%Y-%m-%d %H:%M:%S'

    return f"'{item.strftime(format)}'"


def maybe_enquote_for_server(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        rv = f(*args, **kwargs)

        if not kwargs.get('for_server'):
            return rv

        is_str = isinstance(rv, str)

        nested = kwargs.get('nested')
        item = kwargs['item'] if 'item' in kwargs else args[0]
        if is_str and not isinstance(item, (list, tuple)):
            if rv[0] == "'":
                if nested:
                    return f"\\'{rv[1:-1]}\\'"
                return rv
            if nested:
                return f"\\'{rv}\\'"
            return f"'{rv}'"

        if nested:
            return str(rv)
        return f"'{rv!s}'"

    return wrapper


@maybe_enquote_for_server
def escape_param(
    item, context, for_server=False, nested=False
):
    if item is None:
        return 'NULL'

    elif isinstance(item, datetime):
        return escape_datetime(item, context)

    elif isinstance(item, date):
        return f"'{item.strftime('%Y-%m-%d')}'"

    elif isinstance(item, time):
        return f"'{item.strftime('%H:%M:%S')}'"

    elif isinstance(item, str):
        # We need double escaping for server-side parameters.
        if for_server:
            item = ''.join(escape_chars_map.get(c, c) for c in item)
        return f"'{''.join(escape_chars_map.get(c, c) for c in item)}'"

    elif isinstance(item, list):
        serialized_array = ', '.join(
            str(
                escape_param(
                    x,
                    context,
                    for_server=for_server,
                    nested=True,
                )
            ) for x in item
        )
        return f'[{serialized_array}]'

    elif isinstance(item, tuple):
        serialized_tuple = ', '.join(
            str(
                escape_param(
                    x,
                    context,
                    for_server=for_server,
                    nested=True,
                )
            ) for x in item
        )

        return f'({serialized_tuple})'

    elif isinstance(item, dict):
        serializer = partial(
            escape_param,
            context=context,
            for_server=for_server,
            nested=True,
        )

        serialized_dict = ', '.join(
            f'{serializer(key)!s}: {serializer(value)!s}'
            for key, value in item.items()
        )
        return f'{{{serialized_dict}}}'

    elif isinstance(item, Enum):
        return escape_param(item.value, context, for_server=for_server)

    elif isinstance(item, UUID):
        return f"'{item!s}'"

    else:
        return item


def escape_params(params, context, for_server=False):
    escaped = {}

    for key, value in params.items():
        escaped[key] = escape_param(value, context, for_server=for_server)

    return escaped
