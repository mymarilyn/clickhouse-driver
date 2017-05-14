from datetime import date, datetime

from .testcase import BaseTestCase
from src import errors
from src.client import Client
from src.errors import ServerException


class ErrorsInsertTestCase(BaseTestCase):
    def test_type_mismatch(self):
        with self.create_table('a Float32'):
            with self.assertRaises(errors.TypeMismatchError) as e:
                data = [(date(2012, 10, 25), )]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

            self.assertEqual(e.exception.code, errors.ErrorCodes.TYPE_MISMATCH)

    def test_no_such_column(self):
        with self.create_table('a Float32'):
            with self.assertRaises(ServerException) as e:
                data = [(1, )]
                self.client.execute(
                    'INSERT INTO test (b) VALUES', data
                )

            self.assertEqual(
                e.exception.code, errors.ErrorCodes.NO_SUCH_COLUMN_IN_TABLE
            )

    def test_data_malformed_rows(self):
        with self.create_table('a Int8'), self.assertRaises(TypeError):
            data = [1]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

    def test_data_less_columns_then_expected(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, )]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Expected 2 columns, got 1')

    def test_data_more_columns_then_expected(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2, 3)]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Expected 2 columns, got 3')

    def test_data_different_rows_length(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2), (3, )]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Different rows length')


class ColumnsReadWriteTestCase(BaseTestCase):
    def test_read_write_date_datetime(self):
        with self.create_table('a Date, b DateTime'):
            data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25\t2012-10-25 14:07:19')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_read_write_float(self):
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
                    '1\t2'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (3.4028234663852886e+38, 3.4028235e38),
                (float('inf'), 3.4028235e39),
                (-float('inf'), 3.4028235e39),
                (1, 2)
            ])

    def test_read_write_int_trunc(self):
        with self.create_table('a UInt8'):
            data = [(300, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '44')
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(44, )])

        with self.create_table('a Int8'):
            data = [(-300, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '-44')
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(-44, )])

    def test_read_write_uint_type_mismatch(self):
        data = [(-1, )]
        with self.create_table('a UInt8'):
            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

            self.assertEqual(
                e.exception.code, errors.ErrorCodes.TYPE_MISMATCH
            )

    def test_read_write_int(self):
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
                    '10\t300\t123581321\t123581321345589144'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_read_write_int_corner_cases(self):
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
                    '127\t32767\t2147483647\t9223372036854775807\t0\t0\t0\t0'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)


class BaseCompressionTestCase(BaseTestCase):
    compression = False

    @classmethod
    def create_client(cls):
        return Client(cls.host, cls.port, cls.database, cls.user, cls.password,
                      compression=cls.compression)

    def test_read_write_date_datetime(self):
        if self.compression is None:
            return

        with self.create_table('a Date, b DateTime'):
            data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25\t2012-10-25 14:07:19')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)


class QuickLZReadWriteTestCase(BaseCompressionTestCase):
    compression = 'quicklz'


class LZ4ReadWriteTestCase(BaseCompressionTestCase):
    compression = 'lz4'


class LZ4HCReadWriteTestCase(BaseCompressionTestCase):
    compression = 'lz4hc'


class ZSTDReadWriteTestCase(BaseCompressionTestCase):
    compression = 'zstd'
