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
                "'{}' = {}".format(x.name.replace("'", r"\'"), x.value)
                for x in enum_cls
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

    return cls(Enum(cls.ch_type, _parse_options(params)), **column_options)


def _parse_options(option_string):
    options = dict()
    after_name = False
    escaped = False
    quote_character = None
    name = ''
    value = ''

    for ch in option_string:
        if escaped:
            name += ch
            escaped = False  # accepting escaped character

        elif after_name:
            if ch in (' ', '='):
                pass
            elif ch == ',':
                options[name] = int(value)
                after_name = False
                name = ''
                value = ''  # reset before collecting new option
            else:
                value += ch

        elif quote_character:
            if ch == '\\':
                escaped = True
            elif ch == quote_character:
                quote_character = None
                after_name = True  # start collecting option value
            else:
                name += ch

        else:
            if ch == "'":
                quote_character = ch

    if after_name:
        options.setdefault(name, int(value))  # append word after last comma

    return options
