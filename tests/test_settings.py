from clickhouse_driver.errors import ServerException, ErrorCodes
from tests.testcase import BaseTestCase
from tests.util import require_server_version


class SettingTestCase(BaseTestCase):
    def test_int_apply(self):
        settings = {'max_query_size': 142}

        rv = self.client.execute(
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'max_query_size'",
            settings=settings
        )
        self.assertEqual(rv, [('max_query_size', '142', 1)])

    def test_float_apply(self):
        settings = {'totals_auto_threshold': 1.23}

        rv = self.client.execute(
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'totals_auto_threshold'",
            settings=settings
        )
        self.assertEqual(rv, [('totals_auto_threshold', '1.23', 1)])

    def test_bool_apply(self):
        settings = {'force_index_by_date': 2}

        rv = self.client.execute(
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'force_index_by_date'",
            settings=settings
        )
        self.assertEqual(rv, [('force_index_by_date', '1', 1)])

    @require_server_version(1, 1, 54388)
    def test_char_apply(self):
        settings = {'format_csv_delimiter': 'delimiter'}

        rv = self.client.execute(
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'format_csv_delimiter'",
            settings=settings
        )
        self.assertEqual(rv, [('format_csv_delimiter', 'd', 1)])

    def test_max_threads_apply(self):
        settings = {'max_threads': 100500}

        rv = self.client.execute(
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'max_threads'",
            settings=settings
        )
        self.assertEqual(rv, [('max_threads', '100500', 1)])

        settings = {'max_threads': 'auto'}

        self.client.execute(
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'max_threads'",
            settings=settings
        )

    def test_unknown_setting(self):
        settings = {'unknown_setting': 100500}
        self.client.execute('SHOW tables', settings=settings)

    def test_client_settings(self):
        settings = {'max_query_size': 142}

        with self.created_client(settings=settings) as client:
            rv = client.execute(
                "SELECT name, value, changed FROM system.settings "
                "WHERE name = 'max_query_size'"
            )

        self.assertEqual(rv, [('max_query_size', '142', 1)])

    def test_query_settings_override_client_settings(self):
        client_settings = {'max_query_size': 142}
        query_settings = {'max_query_size': 242}

        with self.created_client(settings=client_settings) as client:
            rv = client.execute(
                "SELECT name, value, changed FROM system.settings "
                "WHERE name = 'max_query_size'",
                settings=query_settings
            )

        self.assertEqual(rv, [('max_query_size', '242', 1)])


class LimitsTestCase(BaseTestCase):
    def test_max_result_rows_apply(self):
        query = 'SELECT number FROM system.numbers LIMIT 10'
        settings = {'max_result_rows': 5}

        with self.assertRaises(ServerException) as e:
            self.client.execute(query, settings=settings)
        # New servers return TOO_MANY_ROWS_OR_BYTES.
        # Old servers return TOO_MANY_ROWS.
        error_codes = {
            ErrorCodes.TOO_MANY_ROWS_OR_BYTES,
            ErrorCodes.TOO_MANY_ROWS
        }
        self.assertIn(e.exception.code, error_codes)

        settings = {'max_result_rows': 5, 'result_overflow_mode': 'break'}
        rv = self.client.execute(query, settings=settings)
        self.assertEqual(len(rv), 10)

        rv = self.client.execute(query)
        self.assertEqual(len(rv), 10)
