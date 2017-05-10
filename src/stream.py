from .block import Block, BlockInfo
from .columns.read import read_column
from .columns.write import write_column
from .reader import read_varint, read_binary_str
from .writer import write_varint, write_binary_str


class BlockOutputStream(object):
    def __init__(self, fout):
        self.fout = fout

        super(BlockOutputStream, self).__init__()

    def write(self, block):
        block.info.write(self.fout)

        columns = block.columns
        rows = block.rows

        write_varint(columns, self.fout)
        write_varint(rows, self.fout)

        for i, (col_name, col_type) in enumerate(block.columns_with_types):
            write_binary_str(col_name, self.fout)
            write_binary_str(col_type, self.fout)

            if rows:
                write_column(col_type, block.data[i], self.fout)


class BlockInputStream(object):
    def __init__(self, fin):
        self.fin = fin

        super(BlockInputStream, self).__init__()

    def read(self):
        info = BlockInfo()
        info.read(self.fin)

        columns = read_varint(self.fin)
        rows = read_varint(self.fin)

        data, names, types = [], [], []

        for i in range(columns):
            column_name = read_binary_str(self.fin)
            column_type = read_binary_str(self.fin)

            names.append(column_name)
            types.append(column_type)

            if rows:
                column = read_column(column_type, rows, self.fin)
                data.append(column)

        return Block(
            columns_with_types=list(zip(names, types)),
            data=data,
            info=info
        )
