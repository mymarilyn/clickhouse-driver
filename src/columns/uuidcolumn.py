from uuid import UUID

import six

from .. import errors
from ..reader import read_binary_uint128
from ..writer import write_binary_uint128
from .base import FormatColumn


class UUIDColumn(FormatColumn):
    ch_type = 'UUID'
    py_types = six.string_types + (UUID, )

    def read(self, buf):
        i = self._read(buf)
        return UUID(int=i)

    def write(self, value, buf):
        try:
            if not isinstance(value, UUID):
                value = UUID(value)

        except ValueError:
            raise errors.CannotParseUuidError(
                "Cannot parse uuid '{}'".format(value)
            )

        self._write(value.int, buf)

    def _read(self, buf):
        return read_binary_uint128(buf)

    def _write(self, x, buf):
        return write_binary_uint128(x, buf)
