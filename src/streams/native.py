from ..block import Block, BlockInfo
from ..columns.service import read_column, write_column
from ..reader import read_varint, read_binary_str
from ..writer import write_varint, write_binary_str
from .. import defines


class BlockOutputStream(object):
    def __init__(self, fout, server_revision):
        self.fout = fout
        self.server_revision = server_revision

        super(BlockOutputStream, self).__init__()

    def reset(self):
        pass

    def write(self, block):
        if self.server_revision >= defines.DBMS_MIN_REVISION_WITH_BLOCK_INFO:
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

        self.finalize()

    def finalize(self):
        self.fout.flush()


class BlockInputStream(object):
    def __init__(self, fin, server_revision):
        self.fin = fin
        self.server_revision = server_revision

        super(BlockInputStream, self).__init__()

    def reset(self):
        pass

    def read(self):
        info = BlockInfo()

        if self.server_revision >= defines.DBMS_MIN_REVISION_WITH_BLOCK_INFO:
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
