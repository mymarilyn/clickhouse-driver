from contextlib import contextmanager
import logging
import socket

from .block import Block
from .blockstreamprofileinfo import BlockStreamProfileInfo
from .clientinfo import ClientInfo
from . import defines
from . import errors
from .progress import Progress
from .protocol import Compression, ClientPacketTypes, ServerPacketTypes
from .queryprocessingstage import QueryProcessingStage
from .reader import read_varint, read_binary_str
from .readhelpers import read_exception
from .compression import get_compressor_cls
from .writer import write_varint, write_binary_str


logger = logging.getLogger(__name__)


class Packet(object):
    def __init__(self):
        self.type = None
        self.block = None
        self.exception = None
        self.progress = None
        self.profile_info = None

        super(Packet, self).__init__()


class ServerInfo(object):
    def __init__(self, name, version_major, version_minor, revision, timezone):
        self.name = name
        self.version_major = version_major
        self.version_minor = version_minor
        self.revision = revision
        self.timezone = timezone

        super(ServerInfo, self).__init__()


class Connection(object):
    def __init__(
            self, host, port=defines.DEFAULT_PORT,
            database='default', user='default', password='',
            client_name=defines.CLIENT_NAME,
            connect_timeout=defines.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC,
            send_receive_timeout=defines.DBMS_DEFAULT_TIMEOUT_SEC,
            sync_request_timeout=defines.DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
            compress_block_size=defines.DEFAULT_COMPRESS_BLOCK_SIZE,
            compression=False
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.client_name = defines.DBMS_NAME + ' ' + client_name
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.sync_request_timeout = sync_request_timeout

        # Use LZ4 compression by default.
        if compression is True:
            compression = 'lz4'

        if compression is False:
            self.compression = Compression.DISABLED
            self.compressor_cls = None
            self.compress_block_size = None
        else:
            self.compression = Compression.ENABLED
            self.compressor_cls = get_compressor_cls(compression)
            self.compress_block_size = compress_block_size

        self.socket = None
        self.fin = None
        self.fout = None

        self.connected = False

        self.server_info = None

        # Block writer/reader
        self.block_in = None
        self.block_out = None

        super(Connection, self).__init__()

    def get_description(self):
        return '{}:{}'.format(self.host, self.port)

    def force_connect(self):

        if not self.connected:
            self.connect()

        elif not self.ping():
            logger.info('Connection was closed, reconnecting.')
            self.connect()

    def connect(self):
        try:
            if self.connected:
                self.disconnect()

            logger.info(
                'Connecting. Database: %s. User: %s', self.database, self.user
            )

            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.connect_timeout)
            self.socket.connect((self.host, self.port))
            self.connected = True
            self.socket.settimeout(self.send_receive_timeout)

            # performance tweak
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            self.fin = self.socket.makefile('rb')
            self.fout = self.socket.makefile('wb')

            self.send_hello()
            self.receive_hello()

            self.block_in = self.get_block_in_stream()
            self.block_out = self.get_block_out_stream()

        except socket.timeout as e:
            self.disconnect()
            raise errors.SocketTimeoutError(
                '{} ({})'.format(e.message, self.get_description())
            )

        except socket.error as e:
            self.disconnect()
            raise errors.NetworkError(
                '{} ({})'.format(e.strerror, self.get_description())
            )

    def reset_state(self):
        self.socket = None
        self.fin = None
        self.fout = None

        self.connected = False

        self.server_info = None

        self.block_in = None
        self.block_out = None

    def disconnect(self):
        if self.connected:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()

        self.reset_state()

    def send_hello(self):
        write_varint(ClientPacketTypes.HELLO, self.fout)
        write_binary_str(self.client_name, self.fout)
        write_varint(defines.DBMS_VERSION_MAJOR, self.fout)
        write_varint(defines.DBMS_VERSION_MINOR, self.fout)
        write_varint(defines.CLIENT_VERSION, self.fout)
        write_binary_str(self.database, self.fout)
        write_binary_str(self.user, self.fout)
        write_binary_str(self.password, self.fout)

        self.fout.flush()

    def receive_hello(self):
        packet_type = read_varint(self.fin)

        if packet_type == ServerPacketTypes.HELLO:
            server_name = read_binary_str(self.fin)
            server_version_major = read_varint(self.fin)
            server_version_minor = read_varint(self.fin)
            server_revision = read_varint(self.fin)

            server_timezone = None
            if server_revision >= \
                    defines.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE:
                server_timezone = read_binary_str(self.fin)

            self.server_info = ServerInfo(
                server_name, server_version_major, server_version_minor,
                server_revision, server_timezone
            )

            logger.info(
                'Connected to %s server version %s.%s.%s', server_name,
                server_version_major, server_version_minor, server_revision
            )

        elif packet_type == ServerPacketTypes.EXCEPTION:
            raise self.receive_exception()

        else:
            self.disconnect()
            message = self.unexpected_packet_message('Hello or Exception',
                                                     packet_type)
            raise errors.UnexpectedPacketFromServerError(message)

    def ping(self):
        timeout = self.sync_request_timeout

        with self.timeout_setter(timeout):
            try:
                write_varint(ClientPacketTypes.PING, self.fout)
                self.fout.flush()

                packet_type = read_varint(self.fin)
                while packet_type == ServerPacketTypes.PROGRESS:
                    self.receive_progress()
                    packet_type = read_varint(self.fin)

                if packet_type != ServerPacketTypes.PONG:
                    msg = self.unexpected_packet_message('Pong', packet_type)
                    raise errors.UnexpectedPacketFromServerError(msg)

            except Exception as e:
                logger.exception(e)
                return False

        return True

    def receive_packet(self):
        packet = Packet()

        packet.type = packet_type = read_varint(self.fin)

        if packet_type == ServerPacketTypes.DATA:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.EXCEPTION:
            packet.exception = self.receive_exception()

        elif packet.type == ServerPacketTypes.PROGRESS:
            packet.progress = self.receive_progress()

        elif packet.type == ServerPacketTypes.PROFILE_INFO:
            packet.profile_info = self.receive_profile_info()

        elif packet_type == ServerPacketTypes.TOTALS:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.EXTREMES:
            packet.block = self.receive_data()

        elif packet_type == ServerPacketTypes.END_OF_STREAM:
            pass

        else:
            self.disconnect()
            raise errors.UnknownPacketFromServerError(
                'Unknown packet {} from server {}'.format(
                    packet_type, self.get_description()
                )
            )

        return packet

    def get_block_in_stream(self):
        revision = self.server_info.revision

        if self.compression:
            from .streams.compressed import CompressedBlockInputStream

            return CompressedBlockInputStream(self.fin, revision)
        else:
            from .streams.native import BlockInputStream

            return BlockInputStream(self.fin, revision)

    def get_block_out_stream(self):
        revision = self.server_info.revision

        if self.compression:
            from .streams.compressed import CompressedBlockOutputStream

            return CompressedBlockOutputStream(
                self.compressor_cls, self.compress_block_size,
                self.fout, revision
            )
        else:
            from .streams.native import BlockOutputStream

            return BlockOutputStream(self.fout, revision)

    def receive_data(self):
        revision = self.server_info.revision

        if revision >= defines.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            read_binary_str(self.fin)

        block = self.block_in.read()
        self.block_in.reset()
        return block

    def receive_exception(self):
        return read_exception(self.fin)

    def receive_progress(self):
        progress = Progress()
        progress.read(self.server_info.revision, self.fin)
        return progress

    def receive_profile_info(self):
        profile_info = BlockStreamProfileInfo()
        profile_info.read(self.fin)
        return profile_info

    def send_data(self, block, table_name=''):
        write_varint(ClientPacketTypes.DATA, self.fout)

        revision = self.server_info.revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            write_binary_str(table_name, self.fout)

        self.block_out.write(block)
        self.block_out.reset()

    def send_query(self, query):
        if not self.connected:
            self.connect()

        write_varint(ClientPacketTypes.QUERY, self.fout)

        query_id = ''
        write_binary_str(query_id, self.fout)

        revision = self.server_info.revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            client_info = ClientInfo(self.client_name)
            client_info.query_kind = ClientInfo.QueryKind.INITIAL_QUERY

            client_info.write(revision, self.fout)

        write_binary_str('', self.fout)  # query settings

        write_varint(QueryProcessingStage.COMPLETE, self.fout)
        write_varint(self.compression, self.fout)

        write_binary_str(query, self.fout)

        logger.info('Query: %s', query)

        self.fout.flush()

    def send_cancel(self):
        write_varint(ClientPacketTypes.CANCEL, self.fout)

        self.fout.flush()

    def send_external_tables(self, tables):
        for table in tables:
            block = Block(table['structure'], table['data'])
            self.send_data(block, table_name=table['name'])

        # Empty block, end of data transfer.
        self.send_data(Block())

    @contextmanager
    def timeout_setter(self, new_timeout):
        old_timeout = self.socket.gettimeout()
        self.socket.settimeout(new_timeout)

        yield

        self.socket.settimeout(old_timeout)

    def unexpected_packet_message(self, expected, packet_type):
        packet_type = ServerPacketTypes.to_str(packet_type)

        return (
            'Unexpected packet from server {} (expected {}, got {})'
            .format(self.get_description(), expected, packet_type)
        )
