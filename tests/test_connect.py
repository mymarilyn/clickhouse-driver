import socket

from mock import patch

from src import errors
from src.client import Client
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
