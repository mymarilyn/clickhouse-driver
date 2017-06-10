from enum import Enum

import six

from .. import errors
from .intcolumn import IntColumn


class EnumColumn(IntColumn):
    py_types = (Enum, ) + six.integer_types + six.string_types
    format = '<b'

    def __init__(self, enum_cls):
        self.enum_cls = enum_cls
        super(EnumColumn, self).__init__()

    def read(self, buf):
        value = super(EnumColumn, self).read(buf)
        return self.enum_cls(value).name

    def write(self, value, buf):
        source_value = value.name if isinstance(value, Enum) else value
        enum_cls = self.enum_cls

        # Check real enum value
        try:
            if isinstance(source_value, six.string_types):
                value = enum_cls[source_value].value
            else:
                value = enum_cls(source_value).value

        except (ValueError, KeyError):
            choices = ', '.join(
                "'{}' = {}".format(x.name, x.value) for x in enum_cls
            )
            enum_str = '{}({})'.format(enum_cls.__name__, choices)

            raise errors.LogicalError(
                "Unknown element '{}' for type {}"
                .format(source_value, enum_str)
            )

        super(EnumColumn, self).write(value, buf)


class Enum8Column(EnumColumn):
    ch_type = 'Enum8'
    format = '<b'


class Enum16Column(EnumColumn):
    ch_type = 'Enum16'
    format = '<h'


def create_enum_column(spec):
    if spec.startswith('Enum8'):
        params = spec[6:-1]
        cls = Enum8Column
    else:
        params = spec[7:-1]
        cls = Enum16Column

    d = {}
    for param in params.split(", '"):
        pos = param.rfind("'")
        name = param[:pos].lstrip("'")
        value = int(param[pos + 1:].lstrip(' ='))
        d[name] = value

    return cls(Enum(cls.ch_type, d))
