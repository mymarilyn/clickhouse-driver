import socket

from mock import patch

from src import errors
from src.client import Client
from src.protocol import ClientPacketTypes, ServerPacketTypes
from src.reader import _read_one
from tests.testcase import BaseTestCase


class PacketsTestCase(BaseTestCase):
    @classmethod
    def create_client(cls):
        return Client(cls.host, cls.port, cls.database, 'wrong_user')

    def test_exception_on_hello_packet(self):
        with self.assertRaises(errors.ServerException) as e:
            self.client.execute('SHOW TABLES')

        # Simple exception formatting checks
        exc = e.exception
        self.assertIn('Code:', str(exc))
        self.assertIn('Stack trace:', str(exc))

    def test_packets_to_str(self):
        self.assertEqual(ClientPacketTypes.to_str(2), 'Data')
        self.assertEqual(ClientPacketTypes.to_str(42), 'Unknown packet')

        self.assertEqual(ServerPacketTypes.to_str(4), 'Pong')
        self.assertEqual(ClientPacketTypes.to_str(42), 'Unknown packet')


class SocketErrorTestCase(BaseTestCase):
    def test_network_error(self):
        client = Client('bad-address')

        with self.assertRaises(errors.NetworkError):
            client.execute('SHOW TABLES')

    def test_timeout_error(self):
        def side_effect(*args, **kwargs):
            raise socket.timeout

        with patch('socket.socket') as mocked_socket:
            mocked_socket.return_value.connect.side_effect = side_effect

            with self.assertRaises(errors.SocketTimeoutError):
                self.client.execute('SHOW TABLES')

    def test_transport_not_connection_on_disconnect(self):
        # Create connection.
        self.client.execute('SELECT 1')

        def side_effect(*args, **kwargs):
            # Exception should be caught in graceful disconnect.
            raise socket.error(107, 'Transport endpoint is not connected')

        connection = self.client.connection

        with patch.object(connection, 'ping') as mocked_ping:
            mocked_ping.return_value = False

            with patch.object(connection, 'socket') as mocked_socket:
                mocked_socket.shutdown.side_effect = side_effect

                # New socket should be created.
                rv = self.client.execute('SELECT 1')
                self.assertEqual(rv, [(1, )])

    def test_socket_error_on_ping(self):
        self.client.execute('SELECT 1')

        def side_effect(*args, **kwargs):
            raise socket.error(32, 'Broken pipe')

        with patch.object(self.client.connection, 'fout') as mocked_fout:
            mocked_fout.flush.side_effect = side_effect

            rv = self.client.execute('SELECT 1')
            self.assertEqual(rv, [(1, )])

    def test_ping_got_unexpected_package(self):
        self.client.execute('SELECT 1')

        with patch.object(self.client.connection, 'fin') as mocked_fin:
            # Emulate Exception packet on ping.
            mocked_fin.read.return_value = b'\x02'

            with self.assertRaises(errors.UnexpectedPacketFromServerError):
                self.client.execute('SELECT 1')

    def test_eof_error_on_ping(self):
        self.client.execute('SELECT 1')

        self.raised = False

        def side_effect(*args, **kwargs):
            if not self.raised:
                self.raised = True
                raise EOFError('Unexpected EOF while reading bytes')

            else:
                return _read_one(*args, **kwargs)

        with patch('src.reader._read_one') as mocked_fin:
            mocked_fin.side_effect = side_effect

            rv = self.client.execute('SELECT 1')
            self.assertEqual(rv, [(1, )])
