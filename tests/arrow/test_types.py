import json
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

try:
    import pyarrow as pa
except ImportError:
    pa = None

from tests.arrow.testcase import ArrowBaseTestCase


class IntTestCase(ArrowBaseTestCase):
    def test_int8(self):
        self.assert_arrow_column('Int8', pa.int8(), [-128, 0, 127])

    def test_int16(self):
        self.assert_arrow_column('Int16', pa.int16(), [-32768, 0, 32767])

    def test_int32(self):
        self.assert_arrow_column(
            'Int32', pa.int32(), [-2147483648, 0, 2147483647]
        )

    def test_int64(self):
        self.assert_arrow_column(
            'Int64', pa.int64(), [-9223372036854775808, 0,
                                  9223372036854775807]
        )

    def test_uint8(self):
        self.assert_arrow_column('UInt8', pa.uint8(), [0, 255])

    def test_uint16(self):
        self.assert_arrow_column('UInt16', pa.uint16(), [0, 65535])

    def test_uint32(self):
        self.assert_arrow_column('UInt32', pa.uint32(), [0, 4294967295])

    def test_uint64(self):
        self.assert_arrow_column(
            'UInt64', pa.uint64(), [0, 18446744073709551615]
        )


class FloatTestCase(ArrowBaseTestCase):
    def test_float32(self):
        self.assert_arrow_column('Float32', pa.float32(), [-0.5, 0.0, 0.5])

    def test_float64(self):
        self.assert_arrow_column('Float64', pa.float64(), [-1.5, 0.0, 1.5])


class BoolTestCase(ArrowBaseTestCase):
    required_server_version = (21, 12)

    def test_bool(self):
        self.assert_arrow_column('Bool', pa.bool_(), [True, False])


class StringTestCase(ArrowBaseTestCase):
    def test_string(self):
        self.assert_arrow_column('String', pa.string(), ['a', 'b', 'привет'])

    def test_fixed_string(self):
        self.assert_arrow_column('FixedString(3)', pa.string(), ['abc',
                                                                 'def'])

    def test_strings_as_bytes(self):
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', [(b'ab', ), (b'cd', )]
            )

            with self.created_client(
                    settings={'strings_as_bytes': True}) as client:
                table = client.query_arrow('SELECT a FROM test')

            self.assertEqual(table.schema.field('a').type, pa.binary())
            self.assertEqual(table.column('a').to_pylist(), [b'ab', b'cd'])


class NullableTestCase(ArrowBaseTestCase):
    def test_nullable_int(self):
        self.assert_arrow_column(
            'Nullable(Int32)', pa.int32(), [1, None, 3]
        )

    def test_nullable_float(self):
        self.assert_arrow_column(
            'Nullable(Float64)', pa.float64(), [1.5, None, 3.5]
        )

    def test_nullable_string(self):
        self.assert_arrow_column(
            'Nullable(String)', pa.string(), ['a', None, 'c']
        )

    def test_null_count(self):
        with self.create_table('a Nullable(Int64)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [(1, ), (None, ), (None, ), (4, )]
            )
            table = self.client.query_arrow('SELECT a FROM test')

            self.assertEqual(table.column('a').null_count, 2)

    def test_all_null(self):
        self.assert_arrow_column(
            'Nullable(Int32)', pa.int32(), [None, None]
        )


class DateTestCase(ArrowBaseTestCase):
    def test_date(self):
        self.assert_arrow_column(
            'Date', pa.date32(), [date(1970, 1, 2), date(2024, 2, 29)]
        )


class Date32TestCase(ArrowBaseTestCase):
    required_server_version = (21, 9)

    def test_date32(self):
        self.assert_arrow_column(
            'Date32', pa.date32(), [date(1925, 1, 2), date(2024, 2, 29)]
        )


class DateTimeTestCase(ArrowBaseTestCase):
    def test_datetime_with_timezone(self):
        self.assert_arrow_column(
            "DateTime('UTC')", pa.timestamp('s', tz='UTC'),
            [
                datetime(2024, 1, 1, 12, 30, 45, tzinfo=timezone.utc),
                datetime(1970, 1, 2, 0, 0, 1, tzinfo=timezone.utc)
            ]
        )

    def test_datetime_naive(self):
        # Expected values are taken from plain execute to stay
        # independent of local/server timezone conversions.
        with self.create_table('a DateTime'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [(datetime(2024, 1, 1, 12, 30, 45), )]
            )
            table = self.client.query_arrow('SELECT a FROM test')
            expected = [x[0] for x in self.client.execute(
                'SELECT a FROM test'
            )]

            self.assertEqual(table.schema.field('a').type, pa.timestamp('s'))
            self.assertEqual(table.column('a').to_pylist(), expected)


class DateTime64TestCase(ArrowBaseTestCase):
    required_server_version = (20, 1, 2)

    def test_datetime64_millisecond(self):
        self.assert_arrow_column(
            "DateTime64(3, 'UTC')", pa.timestamp('ms', tz='UTC'),
            [datetime(2024, 1, 1, 12, 30, 45, 123000, tzinfo=timezone.utc)]
        )

    def test_datetime64_microsecond(self):
        self.assert_arrow_column(
            "DateTime64(6, 'UTC')", pa.timestamp('us', tz='UTC'),
            [datetime(2024, 1, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)]
        )


class DecimalTestCase(ArrowBaseTestCase):
    required_server_version = (18, 12, 13)

    def test_decimal(self):
        self.assert_arrow_column(
            'Decimal(9, 5)', pa.decimal128(9, 5),
            [Decimal('123.45678'), Decimal('-123.45678')]
        )

    def test_decimal_high_precision(self):
        self.assert_arrow_column(
            'Decimal(38, 10)', pa.decimal128(38, 10),
            [Decimal('1234567890.1234567890')]
        )


class EnumTestCase(ArrowBaseTestCase):
    def test_enum8(self):
        self.assert_arrow_column(
            "Enum8('hello' = 1, 'world' = 2)", pa.string(),
            ['hello', 'world']
        )

    def test_enum16(self):
        self.assert_arrow_column(
            "Enum16('hello' = 1000, 'world' = 2000)", pa.string(),
            ['hello', 'world']
        )


class UUIDTestCase(ArrowBaseTestCase):
    def test_uuid(self):
        self.assert_arrow_column(
            'UUID', pa.string(),
            [UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d')],
            expected=['c0fcbba9-0752-44ed-a5d6-4dfb4342b89d']
        )


class IPTestCase(ArrowBaseTestCase):
    required_server_version = (19, 3, 3)

    def test_ipv4(self):
        self.assert_arrow_column(
            'IPv4', pa.string(), ['10.0.0.1', '192.168.1.1']
        )

    def test_ipv6(self):
        self.assert_arrow_column(
            'IPv6', pa.string(), ['2001:db8::1', '::1']
        )


class LowCardinalityTestCase(ArrowBaseTestCase):
    required_server_version = (19, 3, 3)

    def test_low_cardinality_string(self):
        self.assert_arrow_column(
            'LowCardinality(String)', pa.string(), ['yes', 'no', 'yes']
        )

    def test_low_cardinality_nullable_string(self):
        self.assert_arrow_column(
            'LowCardinality(Nullable(String))', pa.string(),
            ['yes', None, 'yes']
        )


class ArrayTestCase(ArrowBaseTestCase):
    def test_array_of_ints(self):
        self.assert_arrow_column(
            'Array(UInt64)', pa.list_(pa.uint64()),
            [[1, 2, 3], [], [4]]
        )

    def test_array_of_strings(self):
        self.assert_arrow_column(
            'Array(String)', pa.list_(pa.string()),
            [['a', 'b'], []]
        )

    def test_array_of_nullable(self):
        self.assert_arrow_column(
            'Array(Nullable(Int32))', pa.list_(pa.int32()),
            [[1, None], [3]]
        )

    def test_nested_arrays(self):
        self.assert_arrow_column(
            'Array(Array(Int32))', pa.list_(pa.list_(pa.int32())),
            [[[1, 2], [3]], [[]]]
        )


class JSONTestCase(ArrowBaseTestCase):
    required_server_version = (24, 8, 0)

    def client_kwargs(self, version):
        return {'settings': {'enable_json_type': True}}

    def cli_client_kwargs(self):
        return {'enable_json_type': 1}

    def test_json_as_string(self):
        with self.create_table('a JSON'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [({'k': 1, 's': 'x'}, ), ({}, )]
            )
            table = self.client.query_arrow('SELECT a FROM test')

            self.assertEqual(table.schema.field('a').type, pa.string())
            values = [json.loads(x) for x in table.column('a').to_pylist()]
            self.assertEqual(values, [{'k': 1, 's': 'x'}, {}])

    def test_json_dynamic_paths_across_blocks(self):
        # Paths appearing only in later blocks must not be lost: JSON
        # text keeps the schema stable however paths evolve.
        with self.create_table('a JSON'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', [({'a': 1}, )]
            )
            self.client.execute(
                'INSERT INTO test (a) VALUES', [({'b': 'x'}, )]
            )
            table = self.client.query_arrow('SELECT a FROM test')

            values = [json.loads(x) for x in table.column('a').to_pylist()]
            self.assertEqual(values, [{'a': 1}, {'b': 'x'}])


class MapTestCase(ArrowBaseTestCase):
    required_server_version = (21, 1, 2)

    def client_kwargs(self, version):
        if version < (21, 4):
            return {'settings': {'allow_experimental_map_type': True}}

    def cli_client_kwargs(self):
        if self.server_version < (21, 4):
            return {'allow_experimental_map_type': 1}

    def test_map(self):
        with self.create_table('a Map(String, UInt64)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [({'a': 1, 'b': 2}, ), ({}, )]
            )
            table = self.client.query_arrow('SELECT a FROM test')

            self.assertEqual(
                table.schema.field('a').type,
                pa.map_(pa.string(), pa.uint64())
            )
            self.assertEqual(
                table.column('a').to_pylist(),
                [[('a', 1), ('b', 2)], []]
            )
