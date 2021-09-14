try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class StringTestCase(NumpyBaseTestCase):
    def test_string(self):
        with self.create_table('a String'):
            data = [np.array(['a', 'b', 'c'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'a\nb\nc\n')
            rv = self.client.execute(query, columnar=True)

            self.assertArraysEqual(rv[0], data)
            self.assertNotEqual(rv[0].dtype, object)
            self.assertIsInstance(rv[0][1], (np.str_, ))

    def test_nullable(self):
        with self.create_table('a Nullable(String)'):
            data = [np.array([np.nan, 'test', None, 'nullable'], dtype=object)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '\\N\ntest\n\\N\nnullable\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], [np.array([None, 'test', None, 'nullable'])]
            )
            self.assertEqual(inserted[0].dtype, object)


class ByteStringTestCase(NumpyBaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True, 'use_numpy': True}}

    def test_string(self):
        with self.create_table('a String'):
            data = [np.array([b'a', b'b', b'c'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'a\nb\nc\n')
            rv = self.client.execute(query, columnar=True)

            self.assertArraysEqual(rv[0], data)
            self.assertNotEqual(rv[0].dtype, object)
            self.assertIsInstance(rv[0][0], (np.bytes_, ))

    def test_nullable(self):
        with self.create_table('a Nullable(String)'):
            data = [
                np.array([np.nan, b'test', None, b'nullable'], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '\\N\ntest\n\\N\nnullable\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], [np.array([None, b'test', None, b'nullable'])]
            )
            self.assertEqual(inserted[0].dtype, object)


class FixedStringTestCase(NumpyBaseTestCase):
    def test_string(self):
        with self.create_table('a FixedString(3)'):
            data = [np.array(['a', 'b', 'c'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'a\\0\\0\nb\\0\\0\nc\\0\\0\n')
            rv = self.client.execute(query, columnar=True)

            self.assertArraysEqual(rv[0], data)
            self.assertNotEqual(rv[0].dtype, object)
            self.assertIsInstance(rv[0][0], (np.str_, ))

    def test_nullable(self):
        with self.create_table('a Nullable(FixedString(10))'):
            data = [np.array([np.nan, 'test', None, 'nullable'], dtype=object)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\\N\ntest\\0\\0\\0\\0\\0\\0\n\\N\nnullable\\0\\0\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], [np.array([None, 'test', None, 'nullable'])]
            )
            self.assertEqual(inserted[0].dtype, object)


class ByteFixedStringTestCase(NumpyBaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True, 'use_numpy': True}}

    def test_string(self):
        with self.create_table('a FixedString(3)'):
            data = [np.array([b'a', b'b', b'c'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'a\\0\\0\nb\\0\\0\nc\\0\\0\n')
            rv = self.client.execute(query, columnar=True)

            self.assertArraysEqual(rv[0], data)
            self.assertNotEqual(rv[0].dtype, object)
            self.assertIsInstance(rv[0][0], (np.bytes_, ))

    def test_nullable(self):
        with self.create_table('a Nullable(FixedString(10))'):
            data = [np.array([
                np.nan,
                b'test\x00\x00\x00\x00\x00\x00',
                None,
                b'nullable\x00\x00'
            ], dtype=object)]

            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\\N\ntest\\0\\0\\0\\0\\0\\0\n\\N\nnullable\\0\\0\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], [np.array([None, b'test', None, b'nullable'])]
            )
            self.assertEqual(inserted[0].dtype, object)
