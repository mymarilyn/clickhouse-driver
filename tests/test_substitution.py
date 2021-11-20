# coding=utf-8
from __future__ import unicode_literals

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import Mock
from uuid import UUID

from enum import IntEnum, Enum
from pytz import timezone

from tests.testcase import BaseTestCase
from tests.util import patch_env_tz


class ParametersSubstitutionTestCase(BaseTestCase):
    single_tpl = 'SELECT %(x)s'
    double_tpl = 'SELECT %(x)s, %(y)s'

    def assert_subst(self, tpl, params, sql):
        ctx = Mock()
        ctx.server_info.timezone = 'Europe/Moscow'
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
