try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class FloatTestCase(NumpyBaseTestCase):
    n = 10

    def check_column(self, rv, col_type):
        self.assertArraysEqual(rv[0], np.array(range(self.n)))
        self.assertIsInstance(rv[0][0], (col_type, ))

    def get_query(self, ch_type):
        query = 'SELECT CAST(number AS {}) FROM numbers({})'.format(
            ch_type, self.n
        )

        return self.client.execute(query, columnar=True)

    def test_float32(self):
        rv = self.get_query('Float32')
        self.check_column(rv, np.float32)

    def test_float64(self):
        rv = self.get_query('Float64')
        self.check_column(rv, np.float64)
