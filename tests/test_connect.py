# coding: utf-8
import socket

from mock import patch
from io import BytesIO

from clickhouse_driver import errors
from clickhouse_driver.client import Client
from clickhouse_driver.protocol import ClientPacketTypes, ServerPacketTypes
from clickhouse_driver.bufferedreader import BufferedReader
from clickhouse_driver.writer import write_binary_str
from tests.testcase import BaseTestCase
from unittest import TestCase


class PacketsTestCase(BaseTestCase):
    def test_packets_to_str(self):
        self.assertEqual(ClientPacketTypes.to_str(2), 'Data')
        self.assertEqual(ClientPacketTypes.to_str(6), 'Unknown packet')
        self.assertEqual(ClientPacketTypes.to_str(42), 'Unknown packet')

        self.assertEqual(ServerPacketTypes.to_str(4), 'Pong')
        self.assertEqual(ServerPacketTypes.to_str(12), 'Unknown packet')
        self.assertEqual(ServerPacketTypes.to_str(42), 'Unknown packet')


class ConnectTestCase(BaseTestCase):
    def test_exception_on_hello_packet(self):
        client = Client(self.host, self.port, self.database, 'wrong_user')

        with self.assertRaises(errors.ServerException) as e:
            client.execute('SHOW TABLES')

        client.disconnect()

        # Simple exception formatting checks
        exc = e.exception
        self.assertIn('Code:', str(exc))
        self.assertIn('Stack trace:', str(exc))

    def test_network_error(self):
        client = Client('bad-address')

        with patch('socket.getaddrinfo') as mocked_getaddrinfo:
            mocked_getaddrinfo.side_effect = socket.error(
                -2, 'Name or service not known'
            )

            with self.assertRaises(errors.NetworkError):
                client.execute('SHOW TABLES')

    def test_timeout_error(self):
        with patch('socket.socket') as mocked_socket:
            mocked_socket.return_value.connect.side_effect = socket.timeout

            with self.assertRaises(errors.SocketTimeoutError):
                self.client.execute('SHOW TABLES')

    def test_transport_not_connection_on_disconnect(self):
        # Create connection.
        self.client.execute('SELECT 1')

        connection = self.client.connection

        with patch.object(connection, 'ping') as mocked_ping:
            mocked_ping.return_value = False

            with patch.object(connection, 'socket') as mocked_socket:
                mocked_socket.shutdown.side_effect = socket.error(
                    107, 'Transport endpoint is not connected'
                )

                # New socket should be created.
                rv = self.client.execute('SELECT 1')
                self.assertEqual(rv, [(1, )])

                # Close newly created socket.
                connection.socket.close()

    def test_socket_error_on_ping(self):
        self.client.execute('SELECT 1')

        with patch.object(self.client.connection, 'fout') as mocked_fout:
            mocked_fout.flush.side_effect = socket.error(32, 'Broken pipe')

            rv = self.client.execute('SELECT 1')
            self.assertEqual(rv, [(1, )])

    def test_ping_got_unexpected_package(self):
        self.client.execute('SELECT 1')

        with patch.object(self.client.connection, 'fin') as mocked_fin:
            # Emulate Exception packet on ping.
            mocked_fin.read_one.return_value = 2

            with self.assertRaises(errors.UnexpectedPacketFromServerError):
                self.client.execute('SELECT 1')

    def test_eof_on_receive_packet(self):
        self.client.execute('SELECT 1')

        with patch.object(self.client.connection, 'fin') as mocked_fin:
            # Emulate Exception packet on ping.
            mocked_fin.read_one.side_effect = [4, EOFError]

            with self.assertRaises(EOFError):
                self.client.execute('SELECT 1')

    def test_eof_error_on_ping(self):
        self.client.execute('SELECT 1')

        self.raised = False
        read_one = self.client.connection.fin.read_one

        def side_effect(*args, **kwargs):
            if not self.raised:
                self.raised = True
                raise EOFError('Unexpected EOF while reading bytes')

            else:
                return read_one(*args, **kwargs)

        with patch.object(self.client.connection, 'fin') as mocked_fin:
            mocked_fin.read_one.side_effect = side_effect

            rv = self.client.execute('SELECT 1')
            self.assertEqual(rv, [(1, )])

    def test_alt_hosts(self):
        client = Client(
            'wrong_host', 1234, self.database, self.user, self.password,
            alt_hosts='{}:{}'.format(self.host, self.port)
        )

        getaddrinfo = socket.getaddrinfo

        def side_getaddrinfo(host, *args, **kwargs):
            if host == 'wrong_host':
                raise socket.error(-2, 'Name or service not known')
            return getaddrinfo(host, *args, **kwargs)

        with patch('socket.getaddrinfo') as mocked_getaddrinfo:
            mocked_getaddrinfo.side_effect = side_getaddrinfo

            rv = client.execute('SELECT 1')
            self.assertEqual(rv, [(1,)])

        client.disconnect()


class FakeBufferedReader(BufferedReader):
    def __init__(self, inputs, bufsize=128):
        super(FakeBufferedReader, self).__init__(bufsize)
        self._inputs = inputs
        self._counter = 0

    def read_into_buffer(self):
        try:
            value = self._inputs[self._counter]
        except IndexError:
            value = b''
        else:
            self._counter += 1

        self.current_buffer_size = len(value)
        self.buffer[:len(value)] = value

        if self.current_buffer_size == 0:
            raise EOFError('Unexpected EOF while reading bytes')


class TestBufferedReader(TestCase):

    def test_corner_case_read(self):
        rdr = FakeBufferedReader([
            b'\x00' * 10,
            b'\xff' * 10,
        ])

        self.assertEqual(rdr.read(5), b'\x00' * 5)
        self.assertEqual(rdr.read(10), b'\x00' * 5 + b'\xff' * 5)
        self.assertEqual(rdr.read(5), b'\xff' * 5)

        self.assertRaises(EOFError, rdr.read, 10)

    def test_corner_case_read_to_end_of_buffer(self):
        rdr = FakeBufferedReader([
            b'\x00' * 10,
            b'\xff' * 10,
        ])

        self.assertEqual(rdr.read(5), b'\x00' * 5)
        self.assertEqual(rdr.read(5), b'\x00' * 5)
        self.assertEqual(rdr.read(5), b'\xff' * 5)
        self.assertEqual(rdr.read(5), b'\xff' * 5)

        self.assertRaises(EOFError, rdr.read, 10)

    def test_corner_case_exact_buffer(self):
        rdr = FakeBufferedReader([
            b'\x00' * 10,
            b'\xff' * 10,
        ], bufsize=10)

        self.assertEqual(rdr.read(5), b'\x00' * 5)
        self.assertEqual(rdr.read(10), b'\x00' * 5 + b'\xff' * 5)
        self.assertEqual(rdr.read(5), b'\xff' * 5)

    def test_read_strings(self):
        strings = [
            u'Yoyodat' * 10,
            u'Peter Maffay' * 10,
        ]

        buf = BytesIO()
        for name in strings:
            write_binary_str(name, buf)
        buf = buf.getvalue()

        ref_values = tuple(x.encode('utf-8') for x in strings)

        for split in range(1, len(buf) - 1):
            for split_2 in range(split + 1, len(buf) - 2):
                assert buf[:split] + buf[split:split_2] + buf[split_2:] == buf
                bufs = [
                    buf[:split],
                    buf[split:split_2],
                    buf[split_2:],
                ]
                rdr = FakeBufferedReader(bufs, bufsize=4096)
                read_values = rdr.read_strings(2)
                self.assertEqual(repr(ref_values), repr(read_values))
