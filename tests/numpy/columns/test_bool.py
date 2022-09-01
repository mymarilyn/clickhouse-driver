try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class BoolTestCase(NumpyBaseTestCase):
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

    def test_bool(self):
        rv = self.get_query('Bool')
        self.check_result(rv, np.bool_)

    def test_insert_nan_into_non_nullable(self):
        with self.create_table('a Bool'):
            data = [
                np.array([True, np.nan], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                'true\nfalse\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], np.array([True, 0]))
            self.assertEqual(inserted[0].dtype, np.int32)

    def test_nullable(self):
        with self.create_table('a Nullable(Bool)'):
            data = [np.array([False, None, True, None, False])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'false\n\\N\ntrue\n\\N\nfalse\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, object)
