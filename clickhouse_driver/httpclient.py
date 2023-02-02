import threading
from contextlib import closing
import requests

from . import Client
from .block import ColumnOrientedBlock
from .bufferedreader import HttpBufferedSocketReader
from .connection import Connection
from .context import Context
from .reader import read_binary_str
from .varint import read_varint
from .columns.service import read_column


class HttpConnection(Connection):
    def __init__(self, host, port=None):
        self.host = host
        self.port = 8123
        self.context = Context()
        self._lock = threading.Lock()
        self.is_query_executing = False

    def send_external_tables(self, tables, types_check=False):
        pass

    def send_query(self, query, query_id=None):
        query += ' FORMAT Native'
        self.response = requests.post('http://localhost:8123', data=query, stream=True)

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

    # def receive_result(self, *args, **kwargs):
    #     return

    # def execute(self, query, **kwargs):
    #
    #     context = Context()
    #     context.settings = {}
    #     context.client_settings = {'use_numpy': False}
    #
