from ipaddress import IPv4Address, IPv6Address, AddressValueError

from .. import errors
from ..util import compat
from .exceptions import ColumnTypeMismatchException
from .stringcolumn import ByteFixedString
from .intcolumn import UInt32Column


class IPv4Column(UInt32Column):
    ch_type = "IPv4"
    py_types = compat.string_types + (IPv4Address, int)

    def __init__(self, types_check=False, **kwargs):
        # UIntColumn overrides before_write_item and check_item
        # in its __init__ when types_check is True so we force
        # __init__ without it then add the appropriate check method for IPv4
        super(UInt32Column, self).__init__(types_check=False, **kwargs)

        self.types_check_enabled = types_check
        if types_check:

            def check_item(value):
                if isinstance(value, int) and value < 0:
                    raise ColumnTypeMismatchException(value)

                if not isinstance(value, IPv4Address):
                    try:
                        value = IPv4Address(value)
                    except AddressValueError:
                        # Cannot parse input in a valid IPv4
                        raise ColumnTypeMismatchException(value)

            self.check_item = check_item

    def before_write_item(self, value):
        # allow Ipv4 in integer, string or IPv4Address object
        try:
            if isinstance(value, int):
                return value

            if not isinstance(value, IPv4Address):
                value = IPv4Address(value)

            return int(value)
        except AddressValueError:
            raise errors.CannotParseDomainError(
                "Cannot parse IPv4 '{}'".format(value)
            )

    def after_read_item(self, value):
        return IPv4Address(value)


class IPv6Column(ByteFixedString):
    ch_type = "IPv6"
    py_types = compat.string_types + (IPv6Address, bytes)

    def __init__(self, types_check=False, **kwargs):
        super(IPv6Column, self).__init__(16, types_check=types_check, **kwargs)

        if types_check:

            def check_item(value):
                if isinstance(value, bytes) and len(value) != 16:
                    raise ColumnTypeMismatchException(value)

                if not isinstance(value, IPv6Address):
                    try:
                        value = IPv6Address(value)
                    except AddressValueError:
                        # Cannot parse input in a valid IPv6
                        raise ColumnTypeMismatchException(value)

            self.check_item = check_item

    def before_write_item(self, value):
        # allow Ipv6 in bytes or python IPv6Address
        # this is raw bytes (not encoded) in order to fit FixedString(16)
        try:
            if isinstance(value, bytes):
                return value

            if not isinstance(value, IPv6Address):
                value = IPv6Address(value)
            return value.packed
        except AddressValueError:
            raise errors.CannotParseDomainError(
                "Cannot parse IPv6 '{}'".format(value)
            )

    def after_read_item(self, value):
        return IPv6Address(value)
