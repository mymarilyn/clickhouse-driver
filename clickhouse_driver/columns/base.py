from struct import Struct, error as struct_error

from . import exceptions


class Column(object):
    ch_type = None
    py_types = None

    check_item = None
    after_read_item = None
    before_write_item = None

    types_check_enabled = False

    def __init__(self, types_check=False, **kwargs):
        self.nullable = False
        self.types_check_enabled = types_check
        super(Column, self).__init__()

    def make_null_struct(self, n_items):
        return Struct('<{}B'.format(n_items))

    def _read_nulls_map(self, n_items, buf):
        s = self.make_null_struct(n_items)
        return s.unpack(buf.read(s.size))

    def _write_nulls_map(self, items, buf):
        s = self.make_null_struct(len(items))
        items = [x is None for x in items]
        buf.write(s.pack(*items))

    def prepare_null(self, value):
        if self.nullable and value is None:
            return 0, True

        else:
            return value, False

    def check_item_type(self, value):
        if not isinstance(value, self.py_types):
            raise exceptions.ColumnTypeMismatchException(value)

    def prepare_items(self, items):
        before_write = self.before_write_item
        prepare_null = self.prepare_null if self.nullable else False

        check_item = self.check_item
        if self.types_check_enabled:
            check_item_type = self.check_item_type
        else:
            check_item_type = False

        prepared = [None] * len(items)
        for i, x in enumerate(items):
            if prepare_null:
                x, is_null = prepare_null(x)
            else:
                is_null = False

            if not is_null:
                if check_item_type:
                    check_item_type(x)

                if check_item:
                    check_item(x)

                if before_write:
                    x = before_write(x)

            prepared[i] = x

        return prepared

    def write_data(self, items, buf):
        if self.nullable:
            self._write_nulls_map(items, buf)

        self._write_data(items, buf)

    def _write_data(self, items, buf):
        prepared = self.prepare_items(items)
        self.write_items(prepared, buf)

    def write_items(self, items, buf):
        raise NotImplementedError

    def read_data(self, n_items, buf):
        if self.nullable:
            nulls_map = self._read_nulls_map(n_items, buf)
        else:
            nulls_map = None

        return self._read_data(n_items, buf, nulls_map=nulls_map)

    def _read_data(self, n_items, buf, nulls_map=None):
        after_read = self.after_read_item

        items = self.read_items(n_items, buf)

        if nulls_map is not None:
            if after_read:
                items = tuple(
                    (None if is_null else after_read(items[i]))
                    for i, is_null in enumerate(nulls_map)
                )
            else:
                items = tuple(
                    (None if is_null else items[i])
                    for i, is_null in enumerate(nulls_map)
                )

        else:
            if after_read:
                items = (after_read(x) for x in items)

        return tuple(items)

    def read_items(self, n_items, buf):
        raise NotImplementedError


class FormatColumn(Column):
    """
    Uses struct.pack for bulk items writing.
    """

    format = None

    def make_struct(self, n_items):
        return Struct('<{}{}'.format(n_items, self.format))

    def write_items(self, items, buf):
        s = self.make_struct(len(items))
        try:
            buf.write(s.pack(*items))

        except struct_error as e:
            raise exceptions.StructPackException(e)

    def read_items(self, n_items, buf):
        s = self.make_struct(n_items)
        return s.unpack(buf.read(s.size))
