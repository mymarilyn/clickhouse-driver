from datetime import date, timedelta
from decimal import Decimal

from tests.testcase import BaseTestCase
from tests.util import require_server_version


class LowCardinalityTestCase(BaseTestCase):
    stable_support_version = (19, 9, 2)

    def cli_client_kwargs(self):
        current = self.get_current_server_version()
        if current >= self.stable_support_version:
            return {'allow_suspicious_low_cardinality_types': 1}

    @require_server_version(19, 3, 3)
    def test_uint8(self):
        with self.create_table('a LowCardinality(UInt8)'):
            data = [(x, ) for x in range(255)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\n'.join(str(x[0]) for x in data) + '\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_int8(self):
        with self.create_table('a LowCardinality(Int8)'):
            data = [(x - 127, ) for x in range(255)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\n'.join(str(x[0]) for x in data) + '\n'

            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_nullable_int8(self):
        with self.create_table('a LowCardinality(Nullable(Int8))'):
            data = [(None, ), (-1, ), (0, ), (1, ), (None, )]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '\\N\n-1\n0\n1\n\\N\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_date(self):
        with self.create_table('a LowCardinality(Date)'):
            start = date(1970, 1, 1)
            data = [(start + timedelta(x), ) for x in range(300)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_float(self):
        with self.create_table('a LowCardinality(Float)'):
            data = [(float(x),) for x in range(300)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_decimal(self):
        with self.create_table('a LowCardinality(Float)'):
            data = [(Decimal(x),) for x in range(300)]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_array(self):
        with self.create_table('a Array(LowCardinality(Int16))'):
            data = [((100, 500), )]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '[100,500]\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_empty_array(self):
        with self.create_table('a Array(LowCardinality(Int16))'):
            data = [(tuple(), )]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '[]\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_string(self):
        with self.create_table('a LowCardinality(String)'):
            data = [
                ('test', ), ('low', ), ('cardinality', ),
                ('test', ), ('test', ), ('', )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                'test\nlow\ncardinality\ntest\ntest\n\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_fixed_string(self):
        with self.create_table('a LowCardinality(FixedString(12))'):
            data = [
                ('test', ), ('low', ), ('cardinality', ),
                ('test', ), ('test', ), ('', )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

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

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 3, 3)
    def test_nullable_string(self):
        with self.create_table('a LowCardinality(Nullable(String))'):
            data = [
                ('test', ), ('', ), (None, )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                'test\n\n\\N\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
