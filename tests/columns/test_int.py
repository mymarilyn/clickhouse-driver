from tests.testcase import BaseTestCase
from clickhouse_driver import errors
from tests.util import require_server_version


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

            exc = str(e.exception)
            self.assertIn('Column a', exc)
            self.assertIn('types_check=True', exc)

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


class BigIntTestCase(BaseTestCase):
    required_server_version = (20, 8, 2)

    def cli_client_kwargs(self):
        return {'allow_experimental_bigint_types': 1}

    def test_int128(self):
        with self.create_table('a Int128'):
            data = [
                (-170141183460469231731687303715884105728, ),
                (-111111111111111111111111111111111111111, ),
                (123, ),
                (111111111111111111111111111111111111111, ),
                (170141183460469231731687303715884105727, )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '-170141183460469231731687303715884105728\n'
                '-111111111111111111111111111111111111111\n'
                '123\n'
                '111111111111111111111111111111111111111\n'
                '170141183460469231731687303715884105727\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(21, 6)
    def test_uint128(self):
        with self.create_table('a UInt128'):
            data = [
                (0, ),
                (123, ),
                (340282366920938463463374607431768211455, )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '0\n'
                '123\n'
                '340282366920938463463374607431768211455\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_int256(self):
        with self.create_table('a Int256'):
            data = [
                (-57896044618658097711785492504343953926634992332820282019728792003956564819968, ),  # noqa: E501
                (-11111111111111111111111111111111111111111111111111111111111111111111111111111, ),  # noqa: E501
                (123, ),
                (11111111111111111111111111111111111111111111111111111111111111111111111111111, ),  # noqa: E501
                (57896044618658097711785492504343953926634992332820282019728792003956564819967, )  # noqa: E501
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '-57896044618658097711785492504343953926634992332820282019728792003956564819968\n'  # noqa: E501
                '-11111111111111111111111111111111111111111111111111111111111111111111111111111\n'  # noqa: E501
                '123\n'
                '11111111111111111111111111111111111111111111111111111111111111111111111111111\n'  # noqa: E501
                '57896044618658097711785492504343953926634992332820282019728792003956564819967\n'  # noqa: E501
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_uint256(self):
        with self.create_table('a UInt256'):
            data = [
                (0, ),
                (123, ),
                (111111111111111111111111111111111111111111111111111111111111111111111111111111, ),  # noqa: E501
                (115792089237316195423570985008687907853269984665640564039457584007913129639935, )  # noqa: E501
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '0\n'
                '123\n'
                '111111111111111111111111111111111111111111111111111111111111111111111111111111\n'  # noqa: E501
                '115792089237316195423570985008687907853269984665640564039457584007913129639935\n'  # noqa: E501
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
