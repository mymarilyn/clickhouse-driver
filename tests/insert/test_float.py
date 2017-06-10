from tests.testcase import BaseTestCase


class FloatTestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a Float32, b Float64'):
            data = [
                (3.4028235e38, 3.4028235e38),
                (3.4028235e39, 3.4028235e39),
                (-3.4028235e39, 3.4028235e39),
                (1, 2)
            ]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
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
