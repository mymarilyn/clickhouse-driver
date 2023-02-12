import threading
from contextlib import closing
import requests

from . import Client
from .block import ColumnOrientedBlock
from .bufferedreader import HttpBufferedSocketReader
from .bufferedwriter import HttpBufferedSocketWriter
from .connection import Connection, ServerInfo
from .context import Context
from .reader import read_binary_str
from .varint import read_varint, write_varint
from .columns.service import read_column, write_column
from .writer import write_binary_str


class ChunksGen(object):
    def __init__(self):
        self.data = b''

    def free(self):
        self.data = b''

    def store(self, x):
        self.data += x


class HttpConnection(Connection):
    def __init__(self, host, port=None):
        self.host = host
        self.port = 8123
        self.url = 'http://' + self.host + ':' + str(self.port)
        self.context = Context()
        self._lock = threading.Lock()
        self.is_query_executing = False

        self.insert_query = None

        self.context.server_info = self.server_info = ServerInfo(
            23, 1, 2, 9, 54453,
            'Europe/Moscow', 'test'
        )

    def send_external_tables(self, tables, types_check=False):
        pass

    def send_query(self, query, query_id=None):
        if getattr(self, 'insert_query'):
            return
        query += ' FORMAT Native'
        self.response = requests.post(self.url, data=query, stream=True)

    def force_connect(self):
        pass

    def disconnect(self):
        pass

    def receive_result(self):
        with closing(self.response) as response:
            response.raise_for_status()

            fin = HttpBufferedSocketReader(response, 8192)

            while True:
                data = []
                names, types = [], []

                try:
                    n_columns = read_varint(fin)
                    n_rows = read_varint(fin)

                    for i in range(n_columns):
                        column_name = read_binary_str(fin)
                        column_type = read_binary_str(fin)

                        names.append(column_name)
                        types.append(column_type)

                        column = read_column(self.context, column_type, n_rows, fin)
                        data.append(column)
                except StopIteration:
                    break

                yield data, list(zip(names, types))

    def send_blocks(self, blocks_gen):
        inserted_rows = 0

        params = {'query': self.insert_query + ' FORMAT Native'}
        def iter():
            nonlocal inserted_rows

            chunks_gen = ChunksGen()
            fout = HttpBufferedSocketWriter(chunks_gen, 8192)

            for block in blocks_gen:
                chunks_gen.free()
                # We write transposed data.
                n_columns = block.num_columns
                n_rows = block.num_rows

                write_varint(n_columns, fout)
                write_varint(n_rows, fout)

                for i, (col_name, col_type) in enumerate(
                        block.columns_with_types):
                    write_binary_str(col_name, fout)
                    write_binary_str(col_type, fout)

                    if n_columns:
                        try:
                            items = block.get_column_by_index(i)
                        except IndexError:
                            raise ValueError('Different rows length')

                        write_column(self.context, col_name, col_type, items,
                                     fout, types_check=block.types_check)
                fout.flush()
                yield chunks_gen.data
                inserted_rows += block.num_rows

        self.response = requests.post(self.url, params=params, data=iter())

        with closing(self.response) as response:
            response.raise_for_status()

        return inserted_rows


class HttpClient(Client):
    connection_cls = HttpConnection

    def packet_generator(self):
        for data, columns_with_types in self.connection.receive_result():
            block = ColumnOrientedBlock(
                columns_with_types=columns_with_types,
                data=data
            )
            packet = type('HttpBlock', (object, ), {'block': block})
            yield packet

    def receive_sample_block(self):
        table = self.insert_query.split(' ')[2]
        cols = self.insert_query[self.insert_query.index('('):].strip('()')
        cols = cols.split(', ')
        rv = self.execute('DESCRIBE TABLE ' + table)
        self.connection.insert_query = self.insert_query

        return ColumnOrientedBlock(
            columns_with_types=[x[:2] for x in rv if x[0] in cols]
        )

    def receive_end_of_query(self):
        pass

    def process_insert_query(self, *args, **kwargs):
        self.insert_query = args[0]
        rv = super(HttpClient, self).process_insert_query(*args, **kwargs)
        self.connection.insert_query = False
        return rv

    def send_data(self, blocks_gen):
        return self.connection.send_blocks(blocks_gen)
