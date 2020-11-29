try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class IntTestCase(NumpyBaseTestCase):
    n = 10

    def check_result(self, rv, col_type):
        self.assertArraysEqual(rv[0], np.array(range(self.n)))
        self.assertIsInstance(rv[0][0], (col_type, ))

    def get_query(self, ch_type):
        with self.create_table('a {}'.format(ch_type)):
            data = [np.array(range(self.n))]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '\n'.join(str(x) for x in data[0]) + '\n'
            )
            return self.client.execute(query, columnar=True)

    def test_int8(self):
        rv = self.get_query('Int8')
        self.check_result(rv, np.int8)

    def test_int16(self):
        rv = self.get_query('Int16')
        self.check_result(rv, np.int16)

    def test_int32(self):
        rv = self.get_query('Int32')
        self.check_result(rv, np.int32)

    def test_int64(self):
        rv = self.get_query('Int64')
        self.check_result(rv, np.int64)

    def test_uint8(self):
        rv = self.get_query('UInt8')
        self.check_result(rv, np.uint8)

    def test_uint16(self):
        rv = self.get_query('UInt16')
        self.check_result(rv, np.uint16)

    def test_uint32(self):
        rv = self.get_query('UInt32')
        self.check_result(rv, np.uint32)

    def test_uint64(self):
        rv = self.get_query('UInt64')
        self.check_result(rv, np.uint64)
