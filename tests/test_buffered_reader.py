import socket
from unittest import TestCase, mock

from clickhouse_driver.bufferedreader import BufferedSocketReader


class BufferedReaderTestCase(TestCase):
    def test_overflow_signed_int_string_size(self):
        data = b'\xFF\xFE\xFC\xFE\xFE\xFE\xFE\xFE\x29\x80\x40\x00\x00\x01'

        def recv_into(buf):
            size = len(data)
            buf[0:size] = data
            return size

        with mock.patch('socket.socket') as mock_socket:
            mock_socket.return_value.recv_into.side_effect = recv_into
            reader = BufferedSocketReader(socket.socket(), 1024)

            # Trying to allocate huge amount of memory.
            with self.assertRaises(MemoryError):
                reader.read_strings(5, encoding='utf-8')
