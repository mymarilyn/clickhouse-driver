from enum import Enum

from .. import errors
from ..util import compat
from .intcolumn import IntColumn


class EnumColumn(IntColumn):
    py_types = (Enum, ) + compat.integer_types + compat.string_types

    def __init__(self, enum_cls, **kwargs):
        self.enum_cls = enum_cls
        super(EnumColumn, self).__init__(**kwargs)

    def before_write_item(self, value):
        source_value = value.name if isinstance(value, Enum) else value
        enum_cls = self.enum_cls

        # Check real enum value
        try:
            if isinstance(source_value, compat.string_types):
                return enum_cls[source_value].value
            else:
                return enum_cls(source_value).value

        except (ValueError, KeyError):
            choices = ', '.join(
                "'{}' = {}".format(x.name, x.value) for x in enum_cls
            )
            enum_str = '{}({})'.format(enum_cls.__name__, choices)

            raise errors.LogicalError(
                "Unknown element '{}' for type {}"
                .format(source_value, enum_str)
            )

    def after_read_item(self, value):
        return self.enum_cls(value).name


class Enum8Column(EnumColumn):
    ch_type = 'Enum8'
    format = 'b'
    int_size = 1


class Enum16Column(EnumColumn):
    ch_type = 'Enum16'
    format = 'h'
    int_size = 2


def create_enum_column(spec, column_options):
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

    return cls(Enum(cls.ch_type, d), **column_options)
