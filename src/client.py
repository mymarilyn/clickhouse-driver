from .block import Block
from .connection import Connection
from . import errors
from .protocol import ServerPacketTypes


class Client(object):
    def __init__(self, *args, **kwargs):
        self.connection = Connection(*args, **kwargs)
        super(Client, self).__init__()

    def disconnect(self):
        self.connection.disconnect()

    def receive_result(self, with_column_types=False):
        data, columns_with_types = [], []

        while True:
            block = self.receive_block()
            if not block:
                break

            if block is True:
                continue

            # Header block contains no rows. Pick columns from it.
            if block.rows:
                data.extend(block.data)
            elif not columns_with_types:
                columns_with_types = block.columns_with_types

        if with_column_types:
            return data, columns_with_types
        else:
            return data

    def receive_block(self):
        packet = self.connection.receive_packet()

        if packet.type == ServerPacketTypes.EXCEPTION:
            return False

        elif packet.type == ServerPacketTypes.END_OF_STREAM:
            return False

        elif packet.type == ServerPacketTypes.DATA:
            return packet.block

        else:
            return True

    def execute(self, query, params=None, with_column_types=False,
                external_tables=None):
        self.connection.force_connect()

        try:
            is_insert = params is not None
            if is_insert:
                return self.process_insert_query(
                    query, params, external_tables=external_tables
                )
            else:
                return self.process_ordinary_query(
                    query, with_column_types=with_column_types,
                    external_tables=external_tables
                )

        except Exception:
            self.connection.disconnect()
            raise

    def process_ordinary_query(self, query, with_column_types=False,
                               external_tables=None):
        self.connection.send_query(query)
        self.connection.send_external_tables(external_tables or [])
        return self.receive_result(with_column_types=with_column_types)

    def process_insert_query(self, query_without_data, data,
                             external_tables=None):
        self.connection.send_query(query_without_data)
        self.connection.send_external_tables(external_tables or [])

        sample_block = self.receive_sample_block()
        if sample_block:
            self.send_data(sample_block, data)
            packet = self.connection.receive_packet()
            if packet.exception:
                raise packet.exception

    def receive_sample_block(self):
        packet = self.connection.receive_packet()

        if packet.type == ServerPacketTypes.DATA:
            return packet.block

        elif packet.type == ServerPacketTypes.EXCEPTION:
            raise packet.exception

        else:
            message = self.connection.unexpected_packet_message('Data',
                                                                packet.type)
            raise errors.UnexpectedPacketFromServerError(message)

    def send_data(self, sample_block, data):
        Block.check_data_sanity(sample_block.columns_with_types, data)

        block = Block(sample_block.columns_with_types, data)
        self.connection.send_data(block)

        # Empty block means end of data.
        self.connection.send_data(Block())
