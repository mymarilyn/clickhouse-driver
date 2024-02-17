from decimal import Decimal

from clickhouse_driver import errors
from tests.testcase import BaseTestCase
from tests.util import require_server_version


class DecimalTestCase(BaseTestCase):
    required_server_version = (18, 12, 13)
    stable_support_version = (18, 14, 9)
    trailing_zeros_version = (21, 9)

    def client_kwargs(self, version):
        if version < self.stable_support_version:
            return {'settings': {'allow_experimental_decimal_type': True}}

    def cli_client_kwargs(self):
        if self.stable_support_version > self.server_version:
            return {'allow_experimental_decimal_type': 1}

    def test_simple(self):
        with self.create_table('a Decimal(9, 5)'):
            data = [(Decimal('300.42'), ), (300.42, ), (-300, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            if self.server_version < self.trailing_zeros_version:
                expected = '300.42000\n300.42000\n-300.00000\n'
            else:
                expected = '300.42\n300.42\n-300\n'
            self.assertEqual(inserted, expected)
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (Decimal('300.42'), ),
                (Decimal('300.42'), ),
                (Decimal('-300'), )
            ])

    @require_server_version(18, 12, 17)
    def test_different_precisions(self):
        columns = 'a Decimal32(2), b Decimal64(2), c Decimal128(2)'

        with self.create_table(columns):
            data = [(
                Decimal('300.42'),
                # 300.42 + (1 << 34)
                Decimal('17179869484.42'),
                # 300.42 + (1 << 100)
                Decimal('1267650600228229401496703205676.42')
            )]
            self.client.execute(
                'INSERT INTO test (a, b, c) VALUES', data
            )

            # Casting to string saves precision.
            query = (
                'SELECT '
                'CAST(a AS String), CAST(b AS String), CAST(c AS String)'
                'FROM test'
            )
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '300.42\t'
                '17179869484.42\t'
                '1267650600228229401496703205676.42\n'
            )

            inserted = self.client.execute('SELECT * FROM test')
            self.assertEqual(inserted, data)

    @require_server_version(18, 12, 17)
    def test_different_precisions_negative(self):
        columns = 'a Decimal32(2), b Decimal64(2), c Decimal128(2)'

        with self.create_table(columns):
            data = [(
                Decimal('-300.42'),
                # 300.42 + (1 << 34)
                Decimal('-17179869484.42'),
                # 300.42 + (1 << 100)
                Decimal('-1267650600228229401496703205676.42')
            )]
            self.client.execute(
                'INSERT INTO test (a, b, c) VALUES', data
            )

            # Casting to string saves precision.
            query = (
                'SELECT '
                'CAST(a AS String), CAST(b AS String), CAST(c AS String)'
                'FROM test'
            )
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '-300.42\t'
                '-17179869484.42\t'
                '-1267650600228229401496703205676.42\n'
            )

            inserted = self.client.execute('SELECT * FROM test')
            self.assertEqual(inserted, data)

    @require_server_version(18, 12, 17)
    def test_max_precisions(self):
        columns = 'a Decimal32(0), b Decimal64(0), c Decimal128(0)'

        with self.create_table(columns):
            data = [(
                Decimal(10**9 - 1),
                Decimal(10**18 - 1),
                Decimal(10**38 - 1)
            ), (
                Decimal(-10**9 + 1),
                Decimal(-10**18 + 1),
                Decimal(-10**38 + 1)
            )]
            self.client.execute(
                'INSERT INTO test (a, b, c) VALUES', data
            )

            # Casting to string saves precision.
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '999999999\t'
                '999999999999999999\t'
                '99999999999999999999999999999999999999\n'
                '-999999999\t'
                '-999999999999999999\t'
                '-99999999999999999999999999999999999999\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        with self.create_table('a Nullable(Decimal32(3))'):
            data = [(300.42, ), (None, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            if self.server_version < self.trailing_zeros_version:
                expected = '300.420\n\\N\n'
            else:
                expected = '300.42\n\\N\n'
            self.assertEqual(inserted, expected)

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(Decimal('300.42'), ), (None, ), ])

    def test_no_scale(self):
        with self.create_table('a Decimal32(0)'):
            data = [(2147483647, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2147483647\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(Decimal('2147483647'), )])

    def test_type_mismatch(self):
        data = [(2147483649, )]
        with self.create_table('a Decimal32(0)'):
            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

            self.assertIn('2147483649 for column "a"', str(e.exception))

            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

            self.assertIn('Column a', str(e.exception))

    def test_preserve_precision(self):
        data = [(1.66, ), (1.15, )]

        with self.create_table('a Decimal(18, 2)'):
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1.66\n1.15\n')
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (Decimal('1.66'), ),
                (Decimal('1.15'), )
            ])

    def test_precision_one_sign_after_point(self):
        data = [(1.6, ), (1.0, ), (12312.0, ), (999999.6, )]

        with self.create_table('a Decimal(8, 1)'):
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            if self.server_version < self.trailing_zeros_version:
                expected = '1.6\n1.0\n12312.0\n999999.6\n'
            else:
                expected = '1.6\n1\n12312\n999999.6\n'
            self.assertEqual(inserted, expected)
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (Decimal('1.6'),),
                (Decimal('1.0'),),
                (Decimal('12312.0'),),
                (Decimal('999999.6'),)
            ])

    def test_truncates_scale(self):
        with self.create_table('a Decimal(9, 4)'):
            data = [(3.14159265358,), (2.7182,)]
            expected = [(Decimal('3.1415'),), (Decimal('2.7182'),)]
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                data,
                types_check=True,
            )
            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, expected)


class Decimal256TestCase(BaseTestCase):
    required_server_version = (18, 12, 13)

    def cli_client_kwargs(self):
        return {'allow_experimental_bigint_types': 1}

    @require_server_version(20, 9, 2)
    def test_decimal256(self):
        data = [
            (1.66, ),
            (1.15, ),
            # 300.42 + (1 << 200)
            (Decimal('1606938044258990275541962092341162602522202993782792835301676.42'), ),  # noqa: E501
            (Decimal('-1606938044258990275541962092341162602522202993782792835301676.42'), )  # noqa: E501
        ]

        with self.create_table('a Decimal256(2)'):
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '1.66\n'
                '1.15\n'
                '1606938044258990275541962092341162602522202993782792835301676.42\n'  # noqa: E501
                '-1606938044258990275541962092341162602522202993782792835301676.42\n'  # noqa: E501
            )
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (Decimal('1.66'), ),
                (Decimal('1.15'), ),
                (Decimal('1606938044258990275541962092341162602522202993782792835301676.42'), ),  # noqa: E501
                (Decimal('-1606938044258990275541962092341162602522202993782792835301676.42'), )  # noqa: E501
            ])
