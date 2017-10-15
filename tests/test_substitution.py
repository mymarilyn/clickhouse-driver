# coding=utf-8
from __future__ import unicode_literals

from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from tests.testcase import BaseTestCase


class ParametersSubstitutionTestCase(BaseTestCase):
    single_tpl = 'SELECT %(x)s'
    double_tpl = 'SELECT %(x)s, %(y)s'

    def assert_subst(self, tpl, params, sql):
        self.assertEqual(self.client.substitute_params(tpl, params), sql)

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
        self.assertEqual(rv, [((1, None, 2), )])

        params = {'x': [[1, 2, 3], [4, 5], [6, 7]]}

        self.assert_subst(self.single_tpl, params,
                          'SELECT [[1, 2, 3], [4, 5], [6, 7]]')

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [(((1, 2, 3), (4, 5), (6, 7)), )])

    def test_tuple(self):
        params = {'x': (1, None, 2)}

        self.assert_subst(self.single_tpl, params, 'SELECT [1, NULL, 2]')

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [((1, None, 2), )])

        params = {'x': ((1, 2, 3), (4, 5), (6, 7))}

        self.assert_subst(self.single_tpl, params,
                          'SELECT [[1, 2, 3], [4, 5], [6, 7]]')

        rv = self.client.execute(self.single_tpl, params)
        self.assertEqual(rv, [(((1, 2, 3), (4, 5), (6, 7)), )])

    def test_enum(self):

        class A(Enum):
            hello = -1
            world = 2

        params = {'x': A.hello, 'y': A.world}

        self.assert_subst(self.double_tpl, params, 'SELECT -1, 2')

        rv = self.client.execute(self.double_tpl, params)
        self.assertEqual(rv, [(-1, 2)])

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

    def test_substitute_object(self):
        params = object()

        with self.assertRaises(ValueError) as e:
            self.client.substitute_params(self.single_tpl, params)

        self.assertEqual(e.exception.args[0],
                         'Parameters are expected in dict form')
