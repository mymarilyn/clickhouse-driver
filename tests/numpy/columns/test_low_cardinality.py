try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
except ImportError:
    pd = None


from tests.numpy.testcase import NumpyBaseTestCase

# from decimal import Decimal


class LowCardinalityTestCase(NumpyBaseTestCase):
    required_server_version = (19, 3, 3)
    stable_support_version = (19, 9, 2)

    def cli_client_kwargs(self):
        if self.server_version >= self.stable_support_version:
            return {'allow_suspicious_low_cardinality_types': 1}

    def check_result(self, inserted, data):
        self.assertArraysEqual(inserted[0], data[0])
        self.assertIsInstance(inserted[0], pd.Categorical)

    def test_uint8(self):
        with self.create_table('a LowCardinality(UInt8)'):
            data = [np.array(range(255))]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\n'.join(str(x) for x in data[0]) + '\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_int8(self):
        with self.create_table('a LowCardinality(Int8)'):
            data = [np.array([x - 127 for x in range(255)])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\n'.join(str(x) for x in data[0]) + '\n'

            )

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_nullable_int8(self):
        with self.create_table('a LowCardinality(Nullable(Int8))'):
            data = [np.array([None, -1, 0, 1, None], dtype=object)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '\\N\n-1\n0\n1\n\\N\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0].astype(str),
                pd.Categorical(data[0]).astype(str)
            )
            self.assertIsInstance(inserted[0], pd.Categorical)

    def test_date(self):
        with self.create_table('a LowCardinality(Date)'):
            data = [np.array(list(range(300)), dtype='datetime64[D]')]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_float(self):
        with self.create_table('a LowCardinality(Float)'):
            data = [np.array([float(x) for x in range(300)])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    # def test_decimal(self):
    #     with self.create_table('a LowCardinality(Float)'):
    #         data = [(Decimal(x),) for x in range(300)]
    #         self.client.execute('INSERT INTO test (a) VALUES', data[0])
    #
    #         query = 'SELECT * FROM test'
    #         inserted = self.client.execute(query)
    #         self.assertEqual(inserted, data[0])
    #
    # def test_array(self):
    #     with self.create_table('a Array(LowCardinality(Int16))'):
    #         data = [([100, 500], )]
    #         self.client.execute('INSERT INTO test (a) VALUES', data[0])
    #
    #         query = 'SELECT * FROM test'
    #         inserted = self.emit_cli(query)
    #         self.assertEqual(inserted, '[100,500]\n')
    #
    #         inserted = self.client.execute(query)
    #         self.assertEqual(inserted, data[0])
    #
    # def test_empty_array(self):
    #     with self.create_table('a Array(LowCardinality(Int16))'):
    #         data = [([], )]
    #         self.client.execute('INSERT INTO test (a) VALUES', data[0])
    #
    #         query = 'SELECT * FROM test'
    #         inserted = self.emit_cli(query)
    #         self.assertEqual(inserted, '[]\n')
    #
    #         inserted = self.client.execute(query)
    #         self.assertEqual(inserted, data[0])
    #
    def test_string(self):
        with self.create_table('a LowCardinality(String)'):
            data = [
                np.array(['test', 'low', 'cardinality', 'test', 'test', ''])
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                'test\nlow\ncardinality\ntest\ntest\n\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_insert_nan_string_into_non_nullable(self):
        with self.create_table('a LowCardinality(String)'):
            data = [
                np.array(['test', None], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                'test\n\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, [np.array(['test', ''])])

    def test_fixed_string(self):
        with self.create_table('a LowCardinality(FixedString(12))'):
            data = [
                np.array(['test', 'low', 'cardinality', 'test', 'test', ''])
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                'test\\0\\0\\0\\0\\0\\0\\0\\0\n'
                'low\\0\\0\\0\\0\\0\\0\\0\\0\\0\n'
                'cardinality\\0\n'
                'test\\0\\0\\0\\0\\0\\0\\0\\0\n'
                'test\\0\\0\\0\\0\\0\\0\\0\\0\n'
                '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.check_result(inserted, data)

    def test_nullable_string(self):
        with self.create_table('a LowCardinality(Nullable(String))'):
            data = [
                np.array(['test', '', None], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                'test\n\n\\N\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0].astype(str), pd.Categorical(data[0]).astype(str)
            )
            self.assertIsInstance(inserted[0], pd.Categorical)
