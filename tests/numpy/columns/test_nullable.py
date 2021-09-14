try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
except ImportError:
    pd = None

from tests.numpy.testcase import NumpyBaseTestCase
from clickhouse_driver import errors

ErrorCodes = errors.ErrorCodes


class NullableTestCase(NumpyBaseTestCase):
    def test_simple(self):
        columns = 'a Nullable(Int32)'

        data = [np.array([3, None, 2], dtype=object)]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '3\n\\N\n2\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, object)

    def test_simple_dataframe(self):
        columns = (
            'a Int64, '
            'b Nullable(Float64), '
            'c Nullable(String), '
            'd Nullable(Int64)'
        )

        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [1.0, None, np.nan],
            'c': ['a', None, np.nan],
            'd': [1, None, None],
        }, dtype=object)
        expected = pd.DataFrame({
            'a': np.array([1, 2, 3], dtype=np.int64),
            'b': np.array([1.0, None, np.nan], dtype=object),
            'c': np.array(['a', None, None], dtype=object),
            'd': np.array([1, None, None], dtype=object),
        })

        with self.create_table(columns):
            rv = self.client.insert_dataframe('INSERT INTO test VALUES', df)
            self.assertEqual(rv, 3)
            df2 = self.client.query_dataframe('SELECT * FROM test ORDER BY a')
            self.assertTrue(expected.equals(df2))
