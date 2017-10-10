from .reader import read_varint, read_binary_uint8, read_binary_int32
from .writer import write_varint, write_binary_uint8, write_binary_int32


class BlockInfo(object):
    is_overflows = False
    bucket_num = -1

    def write(self, buf):
        # Set of pairs (`FIELD_NUM`, value) in binary form. Then 0.
        write_varint(1, buf)
        write_binary_uint8(self.is_overflows, buf)

        write_varint(2, buf)
        write_binary_int32(self.bucket_num, buf)

        write_varint(0, buf)

    def read(self, buf):
        while True:
            field_num = read_varint(buf)
            if not field_num:
                break

            if field_num == 1:
                self.is_overflows = bool(read_binary_uint8(buf))

            elif field_num == 2:
                self.bucket_num = read_binary_int32(buf)


class Block(object):
    dict_row_types = (dict, )
    tuple_row_types = (list, tuple)
    supported_row_types = dict_row_types + tuple_row_types

    def __init__(self, columns_with_types=None, data=None, info=None,
                 types_check=False, received_from_server=False):
        self.columns_with_types = columns_with_types or []
        self.data = data or []
        self.types_check = types_check

        if data and not received_from_server:
            # Guessing about whole data format by first row.
            first_row = data[0]

            if self.types_check:
                self.check_row_type(first_row)

            if isinstance(first_row, dict):
                self.dicts_to_rows(data)
            else:
                self.check_rows(data)

        self.info = info or BlockInfo()

        super(Block, self).__init__()

    def dicts_to_rows(self, data):
        column_names = [x[0] for x in self.columns_with_types]

        check_row_type = False
        if self.types_check:
            check_row_type = self.check_dict_row_type

        for i, row in enumerate(data):
            if check_row_type:
                check_row_type(row)

            self.data[i] = [row[name] for name in column_names]

    def check_rows(self, data):
        expected_row_len = len(self.columns_with_types)

        got = len(data[0])
        if expected_row_len != got:
            msg = 'Expected {} columns, got {}'.format(expected_row_len, got)
            raise ValueError(msg)

        if self.types_check:
            check_row_type = self.check_tuple_row_type
            for row in data:
                check_row_type(row)

    def get_columns(self):
        return self.data

    def get_rows(self):
        if not self.data:
            return self.data

        # Transpose results: columns -> rows.
        n_columns = self.columns
        n_rows = self.rows

        flat_data = [None] * n_columns * n_rows

        for j in range(n_columns):
            column = self.data[j]

            for i in range(n_rows):
                flat_data[i * n_columns + j] = column[i]

        # Make rows from slices.
        rv = [None] * n_rows
        for i in range(n_rows):
            offset = i * n_columns
            rv[i] = tuple(flat_data[offset:offset + n_columns])

        return rv

    def check_row_type(self, row):
        if not isinstance(row, self.supported_row_types):
            raise TypeError(
                'Unsupported row type: {}. dict, list or tuple is expected.'
                .format(type(row))
            )

    def check_tuple_row_type(self, row):
        if not isinstance(row, self.tuple_row_types):
            raise TypeError(
                'Unsupported row type: {}. list or tuple is expected.'
                .format(type(row))
            )

    def check_dict_row_type(self, row):
        if not isinstance(row, self.dict_row_types):
            raise TypeError(
                'Unsupported row type: {}. dict is expected.'
                .format(type(row))
            )

    @property
    def columns(self):
        return len(self.data)

    @property
    def rows(self):
        return len(self.data[0]) if self.columns else 0
