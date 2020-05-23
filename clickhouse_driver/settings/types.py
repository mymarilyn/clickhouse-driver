from ..util.compat import asbool
from ..varint import write_varint
from ..writer import write_binary_str, write_binary_uint8


class SettingType(object):
    @classmethod
    def write_is_important(cls, buf, as_string):
        if as_string:
            write_binary_uint8(0, buf)

    @classmethod
    def write(cls, value, buf, as_string):
        cls.write_is_important(buf, as_string)
        cls.write_value(value, buf, as_string)

    @classmethod
    def write_value(cls, value, buf, as_string):
        raise NotImplementedError


class SettingUInt64(SettingType):
    @classmethod
    def write_value(cls, value, buf, as_string):
        if as_string:
            write_binary_str(str(value), buf)
        else:
            write_varint(int(value), buf)


class SettingBool(SettingType):
    @classmethod
    def write_value(cls, value, buf, as_string):
        value = asbool(value)
        if as_string:
            write_binary_str(str(int(value)), buf)
        else:
            write_varint(value, buf)


class SettingString(SettingType):
    @classmethod
    def write_value(cls, value, buf, as_string):
        write_binary_str(value, buf)


class SettingChar(SettingType):
    @classmethod
    def write_value(cls, value, buf, as_string):
        write_binary_str(value[0], buf)


class SettingFloat(SettingType):
    @classmethod
    def write_value(cls, value, buf, as_string):
        """
        Float is written in string representation.
        """
        write_binary_str(str(value), buf)


class SettingMaxThreads(SettingUInt64):
    @classmethod
    def write_value(cls, value, buf, as_string):
        if as_string:
            write_binary_str(str(value), buf)
        else:
            if value == 'auto':
                value = 0
            super(SettingMaxThreads, cls).write_value(value, buf, as_string)
