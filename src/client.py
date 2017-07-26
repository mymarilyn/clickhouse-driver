from .block import Block
from .connection import Connection
from . import errors
from .protocol import ServerPacketTypes


class QueryResult(object):
    def __init__(self, with_column_types=False):
        self.with_column_types = with_column_types

        self.data = []
        self.columns_with_types = []

        super(QueryResult, self).__init__()

    def get_result(self):
        if self.with_column_types:
            return self.data, self.columns_with_types
        else:
            return self.data


class Progress(object):
    def __init__(self, client_connection, result, progress_gen):
        self.connection = client_connection
        self.result = result
        self.progress_gen = progress_gen

        super(Progress, self).__init__()

    def __iter__(self):
        return self

    def next(self):
        try:
            return next(self.progress_gen)
        except Exception as ex:
            if not isinstance(ex, StopIteration):
                self.connection.disconnect()
            raise

    # For Python 3.
    __next__ = next

    def get_result(self):
        # Read all progress packets.
        for _ in self:
            pass

        return self.result.get_result()


class Client(object):
    def __init__(self, *args, **kwargs):
        self.connection = Connection(*args, **kwargs)
        super(Client, self).__init__()

    def disconnect(self):
        self.connection.disconnect()

    def receive_result(self, with_column_types=False, progress=False):
        result = QueryResult(with_column_types=with_column_types)

        if progress:
            progress_gen = self.receive_progress_result(result)
            return Progress(self.connection, result, progress_gen)

        else:
            self.receive_no_progress_result(result)
            return result.get_result()

    def receive_progress_result(self, result):
        rows_read, approx_rows_to_read = 0, 0
        while True:
            packet = self.receive_packet()
            if not packet:
                break

            if packet is True:
                continue

            progress = getattr(packet, 'progress', None)
            if progress:
                if progress.new_total_rows:
                    approx_rows_to_read = progress.new_total_rows

                rows_read += progress.new_rows

                yield rows_read, approx_rows_to_read

            else:
                self.store_query_result(packet, result)

    def receive_no_progress_result(self, result):
        while True:
            packet = self.receive_packet()
            if not packet:
                break

            if packet is True:
                continue

            self.store_query_result(packet, result)

    def store_query_result(self, packet, result):
        block = getattr(packet, 'block', None)
        if block is None:
            return

        # Header block contains no rows. Pick columns from it.
        if block.rows:
            result.data.extend(block.data)
        elif not result.columns_with_types:
            result.columns_with_types = block.columns_with_types

    def receive_packet(self):
        packet = self.connection.receive_packet()

        if packet.type == ServerPacketTypes.EXCEPTION:
            raise packet.exception

        elif packet.type == ServerPacketTypes.PROGRESS:
            return packet

        elif packet.type == ServerPacketTypes.END_OF_STREAM:
            return False

        elif packet.type == ServerPacketTypes.DATA:
            return packet

        elif packet.type == ServerPacketTypes.TOTALS:
            return packet

        elif packet.type == ServerPacketTypes.EXTREMES:
            return packet

        else:
            return True

    def execute(self, query, params=None, with_column_types=False,
                external_tables=None, query_id=None, settings=None):
        self.connection.force_connect()

        try:
            is_insert = params is not None
            if is_insert:
                return self.process_insert_query(
                    query, params, external_tables=external_tables,
                    query_id=query_id, settings=settings
                )
            else:
                return self.process_ordinary_query(
                    query, with_column_types=with_column_types,
                    external_tables=external_tables,
                    query_id=query_id, settings=settings
                )

        except Exception:
            self.connection.disconnect()
            raise

    def execute_with_progress(self, query, with_column_types=False,
                              external_tables=None, query_id=None,
                              settings=None):
        self.connection.force_connect()

        try:
            return self.process_ordinary_query_with_progress(
                query, with_column_types=with_column_types,
                external_tables=external_tables,
                query_id=query_id, settings=settings
            )

        except Exception:
            self.connection.disconnect()
            raise

    def process_ordinary_query_with_progress(
            self, query, with_column_types=False, external_tables=None,
            query_id=None, settings=None):
        self.connection.send_query(
            query,
            query_id=query_id, settings=settings
        )
        self.connection.send_external_tables(external_tables)
        return self.receive_result(
            with_column_types=with_column_types, progress=True
        )

    def process_ordinary_query(self, query, with_column_types=False,
                               external_tables=None, query_id=None,
                               settings=None):
        self.connection.send_query(
            query,
            query_id=query_id, settings=settings
        )
        self.connection.send_external_tables(external_tables)
        return self.receive_result(with_column_types=with_column_types)

    def process_insert_query(self, query_without_data, data,
                             external_tables=None, query_id=None,
                             settings=None):
        self.connection.send_query(
            query_without_data,
            query_id=query_id, settings=settings
        )
        self.connection.send_external_tables(external_tables)

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

    def cancel(self, with_column_types=False):
        # TODO: Add warning if already cancelled.
        self.connection.send_cancel()
        # Client must still read until END_OF_STREAM packet.
        return self.receive_result(with_column_types=with_column_types)
