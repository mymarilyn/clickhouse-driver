# coding=utf-8
from __future__ import unicode_literals

import struct
import unittest
from datetime import date, datetime, time
from decimal import Decimal
from ipaddress import ip_address
from unittest.mock import Mock
from uuid import UUID, uuid4

from enum import IntEnum, Enum
from pytz import timezone

from tests.testcase import BaseTestCase
from tests.util import patch_env_tz


class ParametersSubstitutionTestCase(BaseTestCase):
    single_tpl = 'SELECT %(x)s'
    double_tpl = 'SELECT %(x)s, %(y)s'

    def assert_subst(self, tpl, params, sql):
        ctx = Mock()
        ctx.server_info.get_timezone.return_value = 'Europe/Moscow'
        self.assertEqual(self.client.substitute_params(tpl, params, ctx), sql)

    def test_int(self):
        params = {'x': 123}

        self.assert_subst(self.single_tpl, params, 'SELECT 123')

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [(123, )])

    def test_null(self):
        params = {'x': None}

        self.assert_subst(self.single_tpl, params, 'SELECT NULL')

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [(None, )])

    def test_date(self):
        d = date(2017, 10, 16)
        params = {'x': d}

        self.assert_subst(self.single_tpl, params, "SELECT '2017-10-16'")

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [('2017-10-16', )])

        tpl = 'SELECT CAST(%(x)s AS Date)'
        self.assert_subst(tpl, params, "SELECT CAST('2017-10-16' AS Date)")

        rv = self.client.execute(tpl, params)
        self.assertEqual(rv, [(d, )])

    def test_time(self):
        t = time(8, 20, 15)
        params = {'x': t}

        self.assert_subst(self.single_tpl, params, "SELECT '08:20:15'")

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [('08:20:15', )])

    def test_datetime(self):
        dt = datetime(2017, 10, 16, 0, 18, 50)
        params = {'x': dt}

        self.assert_subst(self.single_tpl, params,
                          "SELECT '2017-10-16 00:18:50'")

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [('2017-10-16 00:18:50', )])

        tpl = 'SELECT CAST(%(x)s AS DateTime)'
        self.assert_subst(tpl, params,
                          "SELECT CAST('2017-10-16 00:18:50' AS DateTime)")

        rv = self.client.execute(tpl, params)
        self.assertEqual(rv, [(dt, )])

    def test_datetime_with_timezone(self):
        dt = datetime(2017, 7, 14, 5, 40, 0)
        params = {'x': timezone('Asia/Kamchatka').localize(dt)}

        self.assert_subst(self.single_tpl, params,
                          "SELECT '2017-07-13 20:40:00'")

        tpl = (
            'SELECT toDateTime(toInt32(toDateTime(%(x)s))), '
            'toInt32(toDateTime(%(x)s))'
        )

        with patch_env_tz('Asia/Novosibirsk'):
            # use server timezone
            rv = self.client.execute(
                tpl, params, settings={'use_client_time_zone': False}
            )

            self.assertEqual(
                rv, [(datetime(2017, 7, 13, 20, 40, 0), 1499967600)]
            )

            query = (
                "SELECT "
                "toDateTime(toInt32(toDateTime('{0}', 'Asia/Kamchatka'))), "
                "toInt32(toDateTime('{0}', 'Asia/Kamchatka'))"
            ).format('2017-07-14 05:40:00')

            rv = self.emit_cli(query, use_client_time_zone=0)

            self.assertEqual(rv, '2017-07-13 20:40:00\t1499967600\n')

            # use client timezone
            rv = self.client.execute(
                tpl, params, settings={'use_client_time_zone': True}
            )

            self.assertEqual(
                rv, [(datetime(2017, 7, 14, 0, 40, 0), 1499967600)]
            )

            query = (
                "SELECT "
                "toDateTime(toInt32(toDateTime('{0}', 'Asia/Kamchatka'))), "
                "toInt32(toDateTime('{0}', 'Asia/Kamchatka'))"
            ).format('2017-07-14 05:40:00')

            rv = self.emit_cli(query, use_client_time_zone=1)
            self.assertEqual(rv, '2017-07-14 00:40:00\t1499967600\n')

    def test_string(self):
        params = {'x': 'test\t\n\x16', 'y': 'тест\t\n\x16'}

        self.assert_subst(self.double_tpl, params,
                          "SELECT 'test\\t\\n\x16', 'тест\\t\\n\x16'")

        rv = self.client.execute(self.double_tpl, params)
        self.assertEqual(rv, [('test\t\n\x16', 'тест\t\n\x16')])

        params = {'x': "'"}

        self.assert_subst(self.single_tpl, params, "SELECT '\\''")

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [("'", )])

        params = {'x': "\\"}

        self.assert_subst(self.single_tpl, params, "SELECT '\\\\'")

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [("\\", )])

    def test_array(self):
        params = {'x': [1, None, 2]}

        self.assert_subst(self.single_tpl, params, 'SELECT [1, NULL, 2]')

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [([1, None, 2], )])

        params = {'x': [[1, 2, 3], [4, 5], [6, 7]]}

        self.assert_subst(self.single_tpl, params,
                          'SELECT [[1, 2, 3], [4, 5], [6, 7]]')

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [([[1, 2, 3], [4, 5], [6, 7]], )])

    def test_tuple(self):
        params = {'x': (1, None, 2)}

        self.assert_subst('SELECT * FROM test WHERE a IN %(x)s', params,
                          'SELECT * FROM test WHERE a IN (1, NULL, 2)')

        with self.create_table('a Int32'):
            self.client.execute('INSERT INTO test (a) VALUES', [(1, )])
            self.client.execute('INSERT INTO test (a) VALUES', [(2, )])

            query = 'SELECT * FROM test WHERE a IN (1)'

            inserted = self.client.execute(query, columnar=True)
            self.assertEqual(inserted, [(1,)])

    def test_enum(self):

        class A(IntEnum):
            hello = -1
            world = 2

        params = {'x': A.hello, 'y': A.world}

        self.assert_subst(self.double_tpl, params, 'SELECT -1, 2')

        rv = self.client.execute(self.double_tpl, params)
        self.assertEqual(rv, [(-1, 2)])

        class A(Enum):
            hello = 'hello'
            world = 'world'

        params = {'x': A.hello, 'y': A.world}

        self.assert_subst(self.double_tpl, params, "SELECT 'hello', 'world'")

        rv = self.client.execute(self.double_tpl, params)
        self.assertEqual(rv, [('hello', 'world')])

    def test_float(self):
        params = {'x': 1e-12, 'y': 123.45}

        self.assert_subst(self.double_tpl, params, 'SELECT 1e-12, 123.45')

        rv = self.client.execute(self.double_tpl, params)
        self.assertEqual(rv, [(params['x'], params['y'])])

    def test_decimal(self):
        params = {'x': Decimal('1e-2'), 'y': Decimal('123.45')}

        self.assert_subst(self.double_tpl, params, 'SELECT 0.01, 123.45')

        rv = self.client.execute(self.double_tpl, params)
        self.assertEqual(rv, [(0.01, 123.45)])

    def test_uuid(self):
        params = {'x': UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d')}

        self.assert_subst(self.single_tpl, params,
                          "SELECT 'c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'")

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d', )])

    def test_substitute_object(self):
        params = object()

        with self.assertRaises(ValueError) as e:
            self.client.substitute_params(self.single_tpl, params, Mock())

        self.assertEqual(e.exception.args[0],
                         'Parameters are expected in dict form')


class ServerSideParametersSubstitutionTestCase(BaseTestCase):
    required_server_version = (22, 8)

    client_kwargs = {'settings': {'server_side_params': True}}

    def _test_type_aliases(self, x, type_name, type_postfix=''):
        aliases = self.client.execute(
            "SELECT name FROM system.data_type_families "
            f"WHERE alias_to = '{type_name}'"
        )
        for (alias,) in aliases:
            with self.subTest(
                msg=f'{alias}{type_postfix}',
                alias_to=f'{type_name}{type_postfix}',
            ):
                rv = self.client.execute(
                    f'SELECT {{x:{alias}{type_postfix}}}', {'x': x}
                )
                self.assertEqual(rv, [(x, )])

    def _test_type_serialization(self, x, type_pattern, type_postfix=''):
        matching_types = self.client.execute(
            f"SELECT name FROM system.data_type_families "
            f"WHERE match(name, '{type_pattern}')"
        )
        self.assertGreaterEqual(
            len(matching_types), 1, msg='Matching types not found'
        )
        for (matching_type,) in matching_types:
            with self.subTest(msg=f'{matching_type}{type_postfix}'):
                rv = self.client.execute(
                    f'SELECT {{x:{matching_type}{type_postfix}}}', {'x': x}
                )
                self.assertEqual(rv, [(x, )])
            self._test_type_aliases(x, matching_type, type_postfix)

    def test_int(self):
        self._test_type_serialization(123, '^Int\\d+$')

    def test_uint(self):
        self._test_type_serialization(123, '^UInt\\d+$')

    def test_float(self):
        # Make sure float is the same in single and double precision
        x = struct.unpack('=f', struct.pack('=f', 123.45))[0]

        self._test_type_serialization(x, '^Float\\d+$')

    def test_decimal(self):
        x = Decimal(12345) / Decimal(100)
        self._test_type_serialization(x, '^Decimal$', '(5,2)')

    def test_str(self):
        x = "123'"
        self._test_type_serialization(x, '^String$')

    def test_date(self):
        x = date(year=2024, month=1, day=18)
        self._test_type_serialization(x, '^Date\\d*$')

    def test_datetime(self):
        x = datetime(
            year=2024,
            month=1,
            day=18,
            hour=23,
            minute=12,
            second=27,
            microsecond=0,
            tzinfo=None,
        )
        self._test_type_serialization(x, '^DateTime$')

    def test_datetime64(self):
        x = datetime(
            year=2024,
            month=1,
            day=18,
            hour=23,
            minute=12,
            second=27,
            microsecond=123,
            tzinfo=None,
        )
        self._test_type_serialization(x, '^DateTime64$', '(6)')

    def test_enum(self):
        class HelloEnum(Enum):
            hello = 'hello'
        x = HelloEnum.hello
        with self.subTest(msg='Enum'):
            rv = self.client.execute(
                "SELECT {x:Enum('hello')}", {'x': x}
            )
            self.assertEqual(rv, [(x.value, )])
        aliases = self.client.execute(
            "SELECT name FROM system.data_type_families "
            "WHERE alias_to = 'Enum'"
        )
        for (alias,) in aliases:
            with self.subTest(
                msg=f"{alias}('hello')",
                alias_to="Enum('hello')",
            ):
                rv = self.client.execute(
                    f"SELECT {{x:{alias}('hello')}}", {'x': x}
                )
                self.assertEqual(rv, [(x.value, )])

    def test_bool(self):
        x = True
        self._test_type_serialization(x, '^Bool$')

    def test_uuid(self):
        x = uuid4()
        self._test_type_serialization(x, '^UUID')

    def test_ipv4(self):
        x = ip_address('127.0.0.1')
        self._test_type_serialization(x, '^IPv4$')

    def test_ipv6(self):
        x = ip_address('2001:db8::')
        self._test_type_serialization(x, '^IPv6$')

    def test_array__int(self):
        x = [1, 2, 3]
        self._test_type_serialization(x, '^Array$', '(Int32)')

    def test_array__float(self):
        x = [1.23, 2.34, 3.45]
        self._test_type_serialization(x, '^Array$', '(Float64)')

    def test_array__str(self):
        x = ['1', '2', '3']
        self._test_type_serialization(x, '^Array$', '(String)')

    def test_2d_array__int(self):
        x = [[1, 2, 3], [5, 6]]
        self._test_type_serialization(x, '^Array$', '(Array(Int32))')

    def test_2d_array__float(self):
        x = [[1.23, 2.34, 3.45], [5.67, 6.78]]
        self._test_type_serialization(x, '^Array$', '(Array(Float64))')

    def test_2d_array__str(self):
        x = [['1', '2', '3'], ['5', '6']]
        self._test_type_serialization(x, '^Array$', '(Array(String))')

    def test_3d_array__int(self):
        x = [[[1, 2, 3], [5, 6]]]
        self._test_type_serialization(x, '^Array$', '(Array(Array(Int32)))')

    def test_3d_array__float(self):
        x = [[[1.23, 2.34, 3.45], [5.67, 6.78]]]
        self._test_type_serialization(x, '^Array$', '(Array(Array(Float64)))')

    def test_3d_array__str(self):
        x = [[['1', '2', '3'], ['5', '6']]]
        self._test_type_serialization(x, '^Array$', '(Array(Array(String)))')

    def test_tuple(self):
        x = (1, 1.23, '123')
        self._test_type_serialization(x, '^Tuple$', '(Int32, Float64, String)')

    def test_nested_tuple(self):
        x = (1, (1.23, '123'), [('1', 1), ('2', 2), ('3', 3)])
        self._test_type_serialization(
            x,
            '^Tuple$',
            '(Int32, Tuple(Float64, String), Array(Tuple(String, Int32)))'
        )

    def test_map(self):
        x = {1: 2, 3: 4}
        self._test_type_serialization(x, '^Map$', '(UInt32, UInt32)')

    @unittest.skip('Duplicate keys not supported')
    def test_map__duplicate_keys(self):
        pass

    def test_escaped_str(self):
        rv = self.client.execute(
            'SELECT {x:String}, length({x:String})', {'x': '\t'}
        )
        self.assertEqual(rv, [('\t', 1)])

        rv = self.client.execute(
            'SELECT {x:String}, length({x:String})', {'x': '\\'}
        )
        self.assertEqual(rv, [('\\', 1)])

        rv = self.client.execute(
            'SELECT {x:String}, length({x:String})', {'x': "'"}
        )
        self.assertEqual(rv, [("'", 1)])


class NoServerSideParametersSubstitutionTestCase(BaseTestCase):
    def test_reserved_keywords(self):
        self.client.execute(
            'SELECT * FROM system.events LIMIT %(limit)s OFFSET %(offset)s',
            {'limit': 20, 'offset': 30}
        )
