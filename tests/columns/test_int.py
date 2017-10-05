from tests.testcase import BaseTestCase
from src import errors


class IntTestCase(BaseTestCase):
    def test_chop_to_type(self):
        with self.create_table('a UInt8'):
            data = [(300, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '44\n')
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(44, )])

        with self.create_table('a Int8'):
            data = [(-300,)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '-44\n')
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(-44, )])

    def test_raise_struct_error(self):
        with self.create_table('a UInt8'):
            with self.assertRaises(errors.TypeMismatchError) as e:
                data = [(300, )]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

                self.assertIn('Column a', str(e.exception))

    def test_uint_type_mismatch(self):
        data = [(-1, )]
        with self.create_table('a UInt8'):
            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

            self.assertIn('-1 for column "a"', str(e.exception))

            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

            self.assertIn('Column a', str(e.exception))

    def test_all_sizes(self):
        columns = (
            'a Int8, b Int16, c Int32, d Int64, '
            'e UInt8, f UInt16, g UInt32, h UInt64'
        )

        data = [
            (-10, -300, -123581321, -123581321345589144,
             10, 300, 123581321, 123581321345589144)
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b, c, d, e, f, g, h) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    '-10\t-300\t-123581321\t-123581321345589144\t'
                    '10\t300\t123581321\t123581321345589144\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_corner_cases(self):
        columns = (
            'a Int8, b Int16, c Int32, d Int64, '
            'e UInt8, f UInt16, g UInt32, h UInt64'
        )

        data = [
            (-128, -32768, -2147483648, -9223372036854775808,
             255, 65535, 4294967295, 18446744073709551615),
            (127, 32767, 2147483647, 9223372036854775807, 0, 0, 0, 0),
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b, c, d, e, f, g, h) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    '-128\t-32768\t-2147483648\t-9223372036854775808\t'
                    '255\t65535\t4294967295\t18446744073709551615\n'
                    '127\t32767\t2147483647\t9223372036854775807\t0\t0\t0\t0\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        with self.create_table('a Nullable(Int32)'):
            data = [(2, ), (None, ), (4, ), (None, ), (8, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2\n\\N\n4\n\\N\n8\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
