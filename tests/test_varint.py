from io import BytesIO
from unittest import TestCase

from clickhouse_driver.varint import read_varint, write_varint


class VarIntTestCase(TestCase):
    def test_check_not_negative(self):
        n = 0x9FFFFFFF

        buf = BytesIO()
        write_varint(n, buf)
        val = buf.getvalue()
        self.assertEqual(b'\xFF\xFF\xFF\xFF\t', val)

        buf = BytesIO(val)
        buf.read_one = lambda: ord(buf.read(1))
        m = read_varint(buf)
        self.assertEqual(m, n)
