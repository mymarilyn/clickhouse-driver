from . import errors, defines
from .block import Block
from .connection import Connection
from .protocol import ServerPacketTypes
from .result import (
    IterQueryResult, ProgressQueryResult, QueryResult, QueryInfo
)
from .util.escape import escape_params
from .util.helpers import chunks


class Client(object):
    available_client_settings = (
        'insert_block_size',
        'strings_as_bytes'
    )

    def __init__(self, *args, **kwargs):
        self.settings = kwargs.pop('settings', {}).copy()

        self.client_settings = {
            'insert_block_size': self.settings.pop(
                'insert_block_size', defines.DEFAULT_INSERT_BLOCK_SIZE,
            ),
            'strings_as_bytes': self.settings.pop(
                'strings_as_bytes', False
            )
        }

        self.connection = Connection(*args, **kwargs)
        self.connection.context.settings = self.settings
        self.connection.context.client_settings = self.client_settings
        self.reset_last_query()
        super(Client, self).__init__()

    def disconnect(self):
        self.connection.disconnect()
        self.reset_last_query()

    def reset_last_query(self):
        self.last_query = None

    def receive_result(self, with_column_types=False, progress=False,
                       columnar=False):

        gen = self.packet_generator()

        if progress:
            return ProgressQueryResult(
                gen, with_column_types=with_column_types, columnar=columnar
            )

        else:
            result = QueryResult(
                gen, with_column_types=with_column_types, columnar=columnar
            )
            return result.get_result()

    def iter_receive_result(self, with_column_types=False):
        gen = self.packet_generator()

        for rows in IterQueryResult(gen, with_column_types=with_column_types):
            for row in rows:
                yield row

    def packet_generator(self):
        while True:
            try:
                packet = self.receive_packet()
                if not packet:
                    break

                if packet is True:
                    continue

                yield packet

            except Exception:
                self.disconnect()
                raise

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

        elif packet.type == ServerPacketTypes.PROFILE_INFO:
            self.last_query.store_profile(packet)
            return True

        else:
            return True

    def make_query_settings(self, settings):
        settings = settings or {}

        # Pick client-related settings.
        client_settings = self.client_settings.copy()
        for key in self.available_client_settings:
            if key in settings:
                client_settings[key] = settings.pop(key)

        self.connection.context.client_settings = client_settings

        # The rest of settings are ClickHouse-related.
        query_settings = self.settings.copy()
        query_settings.update(settings)
        self.connection.context.settings = query_settings

    def execute(self, query, params=None, with_column_types=False,
                external_tables=None, query_id=None, settings=None,
                types_check=False, columnar=False):

        self.make_query_settings(settings)
        self.connection.force_connect()
        self.last_query = QueryInfo()

        try:
            # INSERT queries can use list or tuple of list/tuples/dicts.
            # For SELECT parameters can be passed in only in dict right now.
            is_insert = isinstance(params, (list, tuple))

            if is_insert:
                return self.process_insert_query(
                    query, params, external_tables=external_tables,
                    query_id=query_id, types_check=types_check
                )
            else:
                return self.process_ordinary_query(
                    query, params=params, with_column_types=with_column_types,
                    external_tables=external_tables,
                    query_id=query_id, types_check=types_check,
                    columnar=columnar
                )

        except Exception:
            self.disconnect()
            raise

    def execute_with_progress(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None, settings=None,
            types_check=False):

        self.make_query_settings(settings)
        self.connection.force_connect()
        self.last_query = QueryInfo()

        try:
            return self.process_ordinary_query_with_progress(
                query, params=params, with_column_types=with_column_types,
                external_tables=external_tables,
                query_id=query_id, types_check=types_check
            )

        except Exception:
            self.disconnect()
            raise

    def execute_iter(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None, settings=None,
            types_check=False):

        self.make_query_settings(settings)
        self.connection.force_connect()
        self.last_query = QueryInfo()

        try:
            return self.iter_process_ordinary_query(
                query, params=params, with_column_types=with_column_types,
                external_tables=external_tables,
                query_id=query_id, types_check=types_check
            )

        except Exception:
            self.disconnect()
            raise

    def process_ordinary_query_with_progress(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False, columnar=False):

        if params is not None:
            query = self.substitute_params(query, params)

        self.connection.send_query(query, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.receive_result(with_column_types=with_column_types,
                                   progress=True, columnar=columnar)

    def process_ordinary_query(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False, columnar=False):

        if params is not None:
            query = self.substitute_params(query, params)

        self.connection.send_query(query, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.receive_result(with_column_types=with_column_types,
                                   columnar=columnar)

    def iter_process_ordinary_query(
            self, query, params=None, with_column_types=False,
            external_tables=None, query_id=None,
            types_check=False, columnar=False):

        if params is not None:
            query = self.substitute_params(query, params)

        self.connection.send_query(query, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)
        return self.iter_receive_result(with_column_types=with_column_types)

    def process_insert_query(self, query_without_data, data,
                             external_tables=None, query_id=None,
                             types_check=False):
        self.connection.send_query(query_without_data, query_id=query_id)
        self.connection.send_external_tables(external_tables,
                                             types_check=types_check)

        sample_block = self.receive_sample_block()
        if sample_block:
            self.send_data(sample_block, data, types_check=types_check)
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

    def send_data(self, sample_block, data, types_check=False):
        client_settings = self.connection.context.client_settings
        for chunk in chunks(data, client_settings['insert_block_size']):
            block = Block(sample_block.columns_with_types, chunk,
                          types_check=types_check)
            self.connection.send_data(block)

        # Empty block means end of data.
        self.connection.send_data(Block())

    def cancel(self, with_column_types=False):
        # TODO: Add warning if already cancelled.
        self.connection.send_cancel()
        # Client must still read until END_OF_STREAM packet.
        return self.receive_result(with_column_types=with_column_types)

    def substitute_params(self, query, params):
        if not isinstance(params, dict):
            raise ValueError('Parameters are expected in dict form')

        escaped = escape_params(params)
        return query % escaped
