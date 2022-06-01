from parameterized import parameterized

from clickhouse_driver.errors import ServerException, ErrorCodes
from tests.testcase import BaseTestCase
from tests.util import require_server_version


class SettingTestCase(BaseTestCase):
    def test_settings_immutable(self):
        settings = {'strings_encoding': 'utf-8'}

        self.client.execute('SELECT 1', settings=settings)
        self.assertEqual(settings, {'strings_encoding': 'utf-8'})

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
        settings = {'force_index_by_date': 1}

        rv = self.client.execute(
            "SELECT name, value, changed FROM system.settings "
            "WHERE name = 'force_index_by_date'",
            settings=settings
        )
        self.assertEqual(rv, [('force_index_by_date', '1', 1)])

    @require_server_version(1, 1, 54388)
    def test_char_apply(self):
        settings = {'format_csv_delimiter': 'd'}

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
        # For both cases unknown setting will be ignored:
        # - rev >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        #   the setting will be ignored by the server with the warning message
        #   (since clickhouse-server does not ignore only important settings,
        #   the one that has important flag)
        # - otherwise the unknown setting will be ignored by the driver.
        settings = {'unknown_setting': 100500}
        self.client.execute('SELECT 1', settings=settings)

    # DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS is 20.1.2+
    @require_server_version(20, 1, 2)
    def test_unknown_setting_is_important(self):
        # In case of rev >=
        # DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS and setting
        # marked as important, then the query should fail.
        settings = {'unknown_setting': 100500}
        with self.created_client(settings_is_important=True) as client:
            with self.assertRaises(ServerException) as e:
                client.execute('SELECT 1', settings=settings)
            self.assertEqual(e.exception.code, ErrorCodes.UNKNOWN_SETTING)

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


class InputFormatNullTestCase(BaseTestCase):
    # Min stable map version
    required_server_version = (21, 8, 1)

    @parameterized.expand([
        ('a Int8, b String', [(None, None)], [(0, '')], '0\t\n'),
        ('a LowCardinality(String)', [(None, )], [('', )], '\n'),
        ('a Tuple(Int32, Int32)', [(None,)], [((0, 0), )], '(0,0)\n'),
        ('a Array(Array(Int32))', [(None,)], [([[0]],)], '[[0]]\n'),
        ('a Map(String, UInt64)', [(None,)], [({},)], '{}\n'),
        ('a Nested(i Int32)', [(None, )], [([0], )], '[0]\n')
    ])
    def test_input_format_null_as_default(self, spec, data, res, cli_res):
        client_settings = {'input_format_null_as_default': True}

        with self.created_client(settings=client_settings) as client:
            with self.create_table(spec):
                client.execute('INSERT INTO test VALUES', data)

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query)
                self.assertEqual(inserted, cli_res)
                inserted = client.execute(query)
                self.assertEqual(inserted, res)


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
