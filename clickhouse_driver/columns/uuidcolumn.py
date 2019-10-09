from uuid import UUID

from .base import FormatColumn
from .. import errors
from ..util import compat
from ..writer import MAX_UINT64


class UUIDColumn(FormatColumn):
    ch_type = 'UUID'
    py_types = compat.string_types + (UUID, )
    format = 'Q'

    # UUID is stored by two uint64 numbers.
    def write_items(self, items, buf):
        n_items = len(items)

        uint_64_pairs = [None] * 2 * n_items
        for i, x in enumerate(items):
            i2 = 2 * i
            uint_64_pairs[i2] = (x >> 64) & MAX_UINT64
            uint_64_pairs[i2 + 1] = x & MAX_UINT64

        s = self.make_struct(2 * n_items)
        buf.write(s.pack(*uint_64_pairs))

    def read_items(self, n_items, buf):
        # TODO: cythonize
        s = self.make_struct(2 * n_items)
        items = s.unpack(buf.read(s.size))

        uint_128_items = [None] * n_items
        for i in range(n_items):
            i2 = 2 * i
            uint_128_items[i] = (items[i2] << 64) + items[i2 + 1]

        return tuple(uint_128_items)

    def after_read_items(self, items, nulls_map=None):
        if nulls_map is None:
            return tuple(UUID(int=item) for item in items)
        else:
            return tuple(
                (None if is_null else UUID(int=items[i]))
                for i, is_null in enumerate(nulls_map)
            )

    def before_write_item(self, value):
        try:
            if not isinstance(value, UUID):
                value = UUID(value)

        except ValueError:
            raise errors.CannotParseUuidError(
                "Cannot parse uuid '{}'".format(value)
            )

        return value.int
