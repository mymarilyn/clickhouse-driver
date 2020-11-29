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
            self.assertIsInstance(rv[0][0], (object, ))


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
            self.assertIsInstance(rv[0][0], (object, ))


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
            self.assertIsInstance(rv[0][0], (object, ))


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
            self.assertIsInstance(rv[0][0], (object, ))
