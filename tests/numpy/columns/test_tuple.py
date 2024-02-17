try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class TupleTestCase(NumpyBaseTestCase):
    def test_simple(self):
        columns = 'a Tuple(Int32, String)'
        dtype = [('f0', np.int32), ('f1', '<U1')]
        data = [
            np.array([(1, 'a'), (2, 'b')], dtype=dtype)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, "(1,'a')\n(2,'b')\n")

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, dtype)

    def test_tuple_single_element(self):
        columns = 'a Tuple(Int32)'
        dtype = [('f0', np.int32)]
        data = [
            np.array([(1, ), (2, )], dtype=dtype)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, "(1)\n(2)\n")

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, dtype)

    def test_nullable(self):
        with self.create_table('a Tuple(Nullable(Int32), Nullable(String))'):
            dtype = [('f0', object), ('f1', object)]
            data = [
                np.array([
                    (1, 'a'),
                    (2, None), (None, None), (None, 'd'),
                    (5, 'e')
                ], dtype=dtype)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "(1,'a')\n"
                "(2,NULL)\n(NULL,NULL)\n(NULL,'d')\n"
                "(5,'e')\n"
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, dtype)

    def test_nested_tuple_with_common_types(self):
        columns = 'a Tuple(String, Tuple(Int32, String), String)'
        dtype = [
            ('f0', '<U5'),
            ('f1', np.dtype([('f0', np.int32), ('f1', '<U1')])),
            ('f2', '<U4')
        ]
        data = [
            np.array([
                ('one', (1, 'a'), 'two'),
                ('three', (2, 'b'), 'four'),
            ], dtype=dtype)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "('one',(1,'a'),'two')\n"
                "('three',(2,'b'),'four')\n"
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, dtype)
