from datetime import date, datetime
from unittest.mock import patch

from pytz import timezone, utc, UnknownTimeZoneError
import tzlocal

from tests.testcase import BaseTestCase
from tests.util import require_server_version, patch_env_tz


class DateTimeTestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a Date, b DateTime'):
            data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25\t2012-10-25 14:07:19\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_date(self):
        with self.create_table('a Nullable(Date)'):
            data = [
                (None, ), (date(2012, 10, 25), ),
                (None, ), (date(2017, 6, 23), )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '\\N\n2012-10-25\n\\N\n2017-06-23\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_datetime(self):
        with self.create_table('a Nullable(DateTime)'):
            data = [
                (None, ), (datetime(2012, 10, 25, 14, 7, 19), ),
                (None, ), (datetime(2017, 6, 23, 19, 10, 15), )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\\N\n2012-10-25 14:07:19\n\\N\n2017-06-23 19:10:15\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_handle_errors_from_tzlocal(self):
        with patch('tzlocal.get_localzone') as mocked:
            mocked.side_effect = UnknownTimeZoneError()
            self.client.execute('SELECT now()')

        if hasattr(tzlocal, 'get_localzone_name'):
            with patch('tzlocal.get_localzone_name') as mocked:
                mocked.side_effect = None
                self.client.execute('SELECT now()')

    @require_server_version(20, 1, 2)
    def test_datetime64_frac_trunc(self):
        with self.create_table('a DateTime64'):
            data = [(datetime(2012, 10, 25, 14, 7, 19, 125600), )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25 14:07:19.125\n')

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [(datetime(2012, 10, 25, 14, 7, 19, 125000), )]
            )

    def test_insert_integers(self):
        with self.create_table('a DateTime'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', [(1530211034, )]
            )

            query = 'SELECT toUInt32(a), a FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1530211034\t2018-06-28 21:37:14\n')

    @require_server_version(20, 1, 2)
    def test_insert_integers_datetime64(self):
        with self.create_table('a DateTime64'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', [(1530211034123, )]
            )

            query = 'SELECT toUInt32(a), a FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1530211034\t2018-06-28 21:37:14.123\n')

    def test_insert_integer_bounds(self):
        with self.create_table('a DateTime'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [(0, ), (1, ), (1500000000, ), (2**32-1, )]
            )

            query = 'SELECT toUInt32(a) FROM test ORDER BY a'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '0\n1\n1500000000\n4294967295\n')

    @require_server_version(21, 4)
    def test_insert_datetime64_extended_range(self):
        with self.create_table('a DateTime64(0)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [(-1420077600, ), (-1420077599, ),
                 (0, ), (1, ),
                 (9877248000, )]
            )

            query = 'SELECT toInt64(a), a FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '-1420077600\t1925-01-01 00:00:00\n'
                '-1420077599\t1925-01-01 00:00:01\n'
                '0\t1970-01-01 03:00:00\n'
                '1\t1970-01-01 03:00:01\n'
                '9877248000\t2282-12-31 03:00:00\n'
            )
            query = 'SELECT a FROM test ORDER BY a'
            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (datetime(1925, 1, 1, 0, 0, 0), ),
                    (datetime(1925, 1, 1, 0, 0, 1), ),
                    (datetime(1970, 1, 1, 3, 0, 0), ),
                    (datetime(1970, 1, 1, 3, 0, 1), ),
                    (datetime(2282, 12, 31, 3, 0, 0), )
                ]
            )

    @require_server_version(21, 4)
    def test_insert_datetime64_extended_range_pure_ints_out_of_range(self):
        with self.create_table('a DateTime64(0)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [(0, ), (1, ), (-2**63, ), (2**63-1, )]
            )

            query = 'SELECT toInt64(a) FROM test ORDER BY a'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '-9223372036854775808\n0\n1\n9223372036854775807\n'
            )


class DateTimeTimezonesTestCase(BaseTestCase):
    dt_type = 'DateTime'

    # Asia/Kamchatka = UTC+12
    # Asia/Novosibirsk = UTC+7
    # Europe/Moscow = UTC+3

    # 1500010800 second since epoch in Europe/Moscow.
    # 1500000000 second since epoch in UTC.
    dt = datetime(2017, 7, 14, 5, 40)
    dt_tz = timezone('Asia/Kamchatka').localize(dt)

    col_tz_name = 'Asia/Novosibirsk'
    col_tz = timezone(col_tz_name)

    # INSERTs and SELECTs must be the same as clickhouse-client's
    # if column has no timezone.

    def table_columns(self, with_tz=False):
        if not with_tz:
            return 'a {}'.format(self.dt_type)

        return "a {}('{}')".format(self.dt_type, self.col_tz_name)

    def test_use_server_timezone(self):
        # Insert datetime with timezone UTC
        # into column with no timezone
        # using server's timezone (Europe/Moscow)

        # Determine server timezone and calculate expected timestamp.
        server_tz_name = self.client.execute('SELECT timezone()')[0][0]
        offset = timezone(server_tz_name).utcoffset(self.dt).total_seconds()
        timestamp = 1500010800 - int(offset)

        with patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(self.dt, )]
                )

                self.emit_cli(
                    "INSERT INTO test (a) VALUES ('2017-07-14 05:40:00')"
                )

                query = 'SELECT toInt32(a) FROM test'
                inserted = self.emit_cli(query)
                self.assertEqual(inserted, '{ts}\n{ts}\n'.format(ts=timestamp))

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query)
                self.assertEqual(
                    inserted,
                    '2017-07-14 05:40:00\n2017-07-14 05:40:00\n'
                )

                inserted = self.client.execute(query)
                self.assertEqual(inserted, [(self.dt, ), (self.dt, )])

    def test_use_client_timezone(self):
        # Insert datetime with timezone UTC
        # into column with no timezone
        # using client's timezone Asia/Novosibirsk

        settings = {'use_client_time_zone': True}

        with patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(self.dt, )],
                    settings=settings
                )

                self.emit_cli(
                    "INSERT INTO test (a) VALUES ('2017-07-14 05:40:00')",
                    use_client_time_zone=1
                )

                query = 'SELECT toInt32(a) FROM test'
                inserted = self.emit_cli(query, use_client_time_zone=1)
                # 1499985600 = 1500000000 - 4 * 3600
                self.assertEqual(inserted, '1499985600\n1499985600\n')

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query, use_client_time_zone=1)
                self.assertEqual(
                    inserted,
                    '2017-07-14 05:40:00\n2017-07-14 05:40:00\n'
                )

                inserted = self.client.execute(query, settings=settings)
                self.assertEqual(inserted, [(self.dt, ), (self.dt, )])

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_use_server_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with no timezone
        # using server's timezone (Europe/Moscow)

        server_tz_name = self.client.execute('SELECT timezone()')[0][0]
        offset = timezone(server_tz_name).utcoffset(self.dt)

        with patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(self.dt_tz, )]
                )

                self.emit_cli(
                    "INSERT INTO test (a) VALUES "
                    "(toDateTime('2017-07-14 05:40:00', 'Asia/Kamchatka'))",
                )

                query = 'SELECT toInt32(a) FROM test'
                inserted = self.emit_cli(query)
                # 1499967600 = 1500000000 - 12 * 3600
                self.assertEqual(inserted, '1499967600\n1499967600\n')

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query)

                dt = (self.dt_tz.astimezone(utc) + offset).replace(tzinfo=None)
                self.assertEqual(inserted, '{dt}\n{dt}\n'.format(dt=dt))

                inserted = self.client.execute(query)
                self.assertEqual(inserted, [(dt, ), (dt, )])

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_use_client_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with no timezone
        # using client's timezone Asia/Novosibirsk

        settings = {'use_client_time_zone': True}

        with patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(self.dt_tz, )],
                    settings=settings
                )

                self.emit_cli(
                    "INSERT INTO test (a) VALUES "
                    "(toDateTime('2017-07-14 05:40:00', 'Asia/Kamchatka'))",
                    use_client_time_zone=1
                )

                query = 'SELECT toInt32(a) FROM test'
                inserted = self.emit_cli(query, use_client_time_zone=1)
                # 1499967600 = 1500000000 - 12 * 3600
                self.assertEqual(inserted, '1499967600\n1499967600\n')

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query, use_client_time_zone=1)
                # 2017-07-14 00:40:00 = 2017-07-14 05:40:00 - 05:00:00
                # (Kamchatka - Novosibirsk)
                self.assertEqual(
                    inserted,
                    '2017-07-14 00:40:00\n2017-07-14 00:40:00\n'
                )

                inserted = self.client.execute(query, settings=settings)
                dt = datetime(2017, 7, 14, 0, 40)
                self.assertEqual(inserted, [(dt, ), (dt, )])

    @require_server_version(1, 1, 54337)
    def test_column_use_server_timezone(self):
        # Insert datetime with no timezone
        # into column with timezone Asia/Novosibirsk
        # using server's timezone (Europe/Moscow)

        with patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(self.dt, )]
                )

                self.emit_cli(
                    "INSERT INTO test (a) VALUES ('2017-07-14 05:40:00')"
                )

                query = 'SELECT toInt32(a) FROM test'
                inserted = self.emit_cli(query)
                # 1499985600 = 1500000000 - 4 * 3600
                self.assertEqual(inserted, '1499985600\n1499985600\n')

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query)
                self.assertEqual(
                    inserted,
                    '2017-07-14 05:40:00\n2017-07-14 05:40:00\n'
                )

                inserted = self.client.execute(query)
                self.assertEqual(inserted, [
                    (self.col_tz.localize(self.dt), ),
                    (self.col_tz.localize(self.dt), )
                ])

    @require_server_version(1, 1, 54337)
    def test_column_use_client_timezone(self):
        # Insert datetime with no timezone
        # into column with timezone Asia/Novosibirsk
        # using client's timezone Europe/Moscow

        settings = {'use_client_time_zone': True}

        with patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(self.dt, )],
                    settings=settings
                )
                self.emit_cli(
                    "INSERT INTO test (a) VALUES ('2017-07-14 05:40:00')",
                    use_client_time_zone=1
                )

                query = 'SELECT toInt32(a) FROM test'
                inserted = self.emit_cli(query, use_client_time_zone=1)
                # 1499985600 = 1500000000 - 4 * 3600
                self.assertEqual(inserted, '1499985600\n1499985600\n')

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query, use_client_time_zone=1)
                self.assertEqual(
                    inserted,
                    '2017-07-14 05:40:00\n2017-07-14 05:40:00\n'
                )

                inserted = self.client.execute(query, settings=settings)
                self.assertEqual(inserted, [
                    (self.col_tz.localize(self.dt), ),
                    (self.col_tz.localize(self.dt), )
                ])

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_column_use_server_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with timezone Asia/Novosibirsk
        # using server's timezone (Europe/Moscow)

        with patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(self.dt_tz, )]
                )

                self.emit_cli(
                    "INSERT INTO test (a) VALUES "
                    "(toDateTime('2017-07-14 05:40:00', 'Asia/Kamchatka'))",
                )

                query = 'SELECT toInt32(a) FROM test'
                inserted = self.emit_cli(query)
                # 1499967600 = 1500000000 - 12 * 3600
                self.assertEqual(inserted, '1499967600\n1499967600\n')

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query)
                # 2017-07-14 00:40:00 = 2017-07-14 05:40:00 - 05:00:00
                # (Kamchatka - Novosibirsk)
                self.assertEqual(
                    inserted,
                    '2017-07-14 00:40:00\n2017-07-14 00:40:00\n'
                )

                inserted = self.client.execute(query)
                dt = datetime(2017, 7, 14, 0, 40)
                self.assertEqual(inserted, [
                    (self.col_tz.localize(dt), ),
                    (self.col_tz.localize(dt), )
                ])

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_column_use_client_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with timezone Asia/Novosibirsk
        # using client's timezone (Europe/Moscow)

        settings = {'use_client_time_zone': True}

        with patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(self.dt_tz, )],
                    settings=settings
                )

                self.emit_cli(
                    "INSERT INTO test (a) VALUES "
                    "(toDateTime('2017-07-14 05:40:00', 'Asia/Kamchatka'))",
                    use_client_time_zone=1
                )

                query = 'SELECT toInt32(a) FROM test'
                inserted = self.emit_cli(query, use_client_time_zone=1)
                # 1499967600 = 1500000000 - 12 * 3600
                self.assertEqual(inserted, '1499967600\n1499967600\n')

                query = 'SELECT * FROM test'
                inserted = self.emit_cli(query, use_client_time_zone=1)
                # 2017-07-14 00:40:00 = 2017-07-14 05:40:00 - 05:00:00
                # (Kamchatka - Novosibirsk)
                self.assertEqual(
                    inserted,
                    '2017-07-14 00:40:00\n2017-07-14 00:40:00\n'
                )

                inserted = self.client.execute(query, settings=settings)
                dt = datetime(2017, 7, 14, 0, 40)
                self.assertEqual(inserted, [
                    (self.col_tz.localize(dt), ),
                    (self.col_tz.localize(dt), )
                ])


class DateTime64TimezonesTestCase(DateTimeTimezonesTestCase):
    dt_type = 'DateTime64'
    required_server_version = (20, 1, 2)

    def table_columns(self, with_tz=False):
        if not with_tz:
            return 'a {}(0)'.format(self.dt_type)

        return "a {}(0, '{}')".format(self.dt_type, self.col_tz_name)
