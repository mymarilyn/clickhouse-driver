from datetime import date, datetime
from enum import Enum
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
    server_tz = timezone(context.server_info.timezone)

    if item.tzinfo is not None:
        item = item.astimezone(server_tz)

    return "'%s'" % item.strftime('%Y-%m-%d %H:%M:%S')


def escape_param(item, context):
    if item is None:
        return 'NULL'

    elif isinstance(item, datetime):
        return escape_datetime(item, context)

    elif isinstance(item, date):
        return "'%s'" % item.strftime('%Y-%m-%d')

    elif isinstance(item, str):
        return "'%s'" % ''.join(escape_chars_map.get(c, c) for c in item)

    elif isinstance(item, list):
        return "[%s]" % ', '.join(str(escape_param(x, context)) for x in item)

    elif isinstance(item, tuple):
        return "(%s)" % ', '.join(str(escape_param(x, context)) for x in item)

    elif isinstance(item, Enum):
        return escape_param(item.value, context)

    elif isinstance(item, UUID):
        return "'%s'" % str(item)

    else:
        return item


def escape_params(params, context):
    escaped = {}

    for key, value in params.items():
        escaped[key] = escape_param(value, context)

    return escaped
