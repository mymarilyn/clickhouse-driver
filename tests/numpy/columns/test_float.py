from parameterized import parameterized

try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class FloatTestCase(NumpyBaseTestCase):
    n = 10

    def check_result(self, rv, col_type):
        self.assertArraysEqual(rv[0], np.array(range(self.n)))
        self.assertEqual(rv[0].dtype, col_type)

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

    def test_float32(self):
        rv = self.get_query('Float32')
        self.check_result(rv, np.float32)

    def test_float64(self):
        rv = self.get_query('Float64')
        self.check_result(rv, np.float64)

    def test_fractional_round_trip(self):
        with self.create_table('a Float32'):
            data = [np.array([0.5, 1.5], dtype=np.float32)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '0.5\n1.5\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])

    @parameterized.expand(['Float32', 'Float64'])
    def test_nullable(self, float_type):
        with self.create_table('a Nullable({})'.format(float_type)):
            data = [np.array([np.nan, 0.5, None, 1.5], dtype=object)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'nan\n0.5\n\\N\n1.5\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0].astype(str), data[0].astype(str)
            )
            self.assertEqual(inserted[0].dtype, object)

    def test_nan(self):
        with self.create_table('a Float32'):
            data = [np.array([float('nan'), 0.5], dtype=np.float32)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'nan\n0.5\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0].astype(str), data[0].astype(str)
            )
            self.assertEqual(inserted[0].dtype, np.float32)
