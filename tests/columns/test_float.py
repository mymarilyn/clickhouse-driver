import math

from tests.testcase import BaseTestCase
from clickhouse_driver import errors


class FloatTestCase(BaseTestCase):
    def test_chop_to_type(self):
        with self.create_table('a Float32, b Float64'):
            data = [
                (3.4028235e38, 3.4028235e38),
                (3.4028235e39, 3.4028235e39),
                (-3.4028235e39, 3.4028235e39),
                (1, 2)
            ]

            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )

            self.assertIn('Column a', str(e.exception))

    def test_simple(self):
        with self.create_table('a Float32, b Float64'):
            data = [
                (3.4028235e38, 3.4028235e38),
                (3.4028235e39, 3.4028235e39),
                (-3.4028235e39, 3.4028235e39),
                (1, 2)
            ]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    '3.4028235e38\t3.4028235e38\n'
                    'inf\t3.4028235e39\n'
                    '-inf\t3.4028235e39\n'
                    '1\t2\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (3.4028234663852886e+38, 3.4028235e38),
                (float('inf'), 3.4028235e39),
                (-float('inf'), 3.4028235e39),
                (1, 2)
            ])

    def test_nullable(self):
        with self.create_table('a Nullable(Float32)'):
            data = [(None, ), (0.5, ), (None, ), (1.5, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '\\N\n0.5\n\\N\n1.5\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nan(self):
        with self.create_table('a Float32'):
            data = [(float('nan'), ), (0.5, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'nan\n0.5\n')

            inserted = self.client.execute(query)
            self.assertEqual(len(inserted), 2)
            self.assertTrue(math.isnan(inserted[0][0]))
            self.assertEqual(inserted[1][0], 0.5)
