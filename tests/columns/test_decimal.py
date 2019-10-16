from decimal import Decimal

from clickhouse_driver import errors
from tests.testcase import BaseTestCase
from tests.util import require_server_version


class DecimalTestCase(BaseTestCase):
    stable_support_version = (18, 14, 9)

    def client_kwargs(self, version):
        if version < self.stable_support_version:
            return {'settings': {'allow_experimental_decimal_type': True}}

    def cli_client_kwargs(self):
        current = self.get_current_server_version()
        if self.stable_support_version > current:
            return {'allow_experimental_decimal_type': 1}

    @require_server_version(18, 12, 13)
    def test_simple(self):
        with self.create_table('a Decimal(9, 5)'):
            data = [(Decimal('300.42'), ), (300.42, ), (-300, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '300.42000\n300.42000\n-300.00000\n')
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

    @require_server_version(18, 12, 13)
    def test_nullable(self):
        with self.create_table('a Nullable(Decimal32(3))'):
            data = [(300.42, ), (None, ), ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '300.420\n\\N\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(Decimal('300.42'), ), (None, ), ])

    @require_server_version(18, 12, 13)
    def test_no_scale(self):
        with self.create_table('a Decimal32(0)'):
            data = [(2147483647, ), ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2147483647\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(Decimal('2147483647'), )])

    @require_server_version(18, 12, 13)
    def test_type_mismatch(self):
        data = [(2147483649,), ]
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
