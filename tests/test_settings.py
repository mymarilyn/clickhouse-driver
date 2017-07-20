from src.errors import ServerException, ErrorCodes
from tests.testcase import BaseTestCase


class SettingTestCase(BaseTestCase):
    def test_int_apply(self):
        settings = {'max_query_size': 142}

        rv = self.client.execute(
            "SELECT * FROM system.settings WHERE name = 'max_query_size'",
            settings=settings
        )
        self.assertEqual(rv, [('max_query_size', '142', 1)])

    def test_float_apply(self):
        settings = {'totals_auto_threshold': 1.23}

        rv = self.client.execute(
            "SELECT * FROM system.settings "
            "WHERE name = 'totals_auto_threshold'",
            settings=settings
        )
        self.assertEqual(rv, [('totals_auto_threshold', '1.23', 1)])

    def test_bool_apply(self):
        settings = {'force_index_by_date': 2}

        rv = self.client.execute(
            "SELECT * FROM system.settings "
            "WHERE name = 'force_index_by_date'",
            settings=settings
        )
        self.assertEqual(rv, [('force_index_by_date', '1', 1)])

    def test_max_threads_apply(self):
        settings = {'max_threads': 100500}

        rv = self.client.execute(
            "SELECT * FROM system.settings "
            "WHERE name = 'max_threads'",
            settings=settings
        )
        self.assertEqual(rv, [('max_threads', '100500', 1)])

        settings = {'max_threads': 'auto'}

        self.client.execute(
            "SELECT * FROM system.settings "
            "WHERE name = 'max_threads'",
            settings=settings
        )


class LimitsTestCase(BaseTestCase):
    def test_max_result_rows_apply(self):
        settings = {'max_result_rows': 5}

        with self.assertRaises(ServerException) as e:
            self.client.execute(
                'SELECT arrayJoin(range(10))',
                settings=settings
            )
        self.assertEqual(e.exception.code, ErrorCodes.TOO_MUCH_ROWS)

        settings = {'max_result_rows': 5, 'result_overflow_mode': 'break'}
        rv = self.client.execute(
            'SELECT arrayJoin(range(10))',
            settings=settings
        )
        self.assertEqual(len(rv), 10)

        rv = self.client.execute('SELECT arrayJoin(range(10))')
        self.assertEqual(len(rv), 10)
