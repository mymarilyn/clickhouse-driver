from contextlib import contextmanager
from datetime import date, datetime
import os
from time import tzset

from mock import patch

try:
    import numpy as np
except ImportError:
    np = None

try:
    import pandas as pd
except ImportError:
    pd = None

from pytz import timezone, utc, UnknownTimeZoneError
import tzlocal

from tests.numpy.testcase import NumpyBaseTestCase
from tests.util import require_server_version


class BaseDateTimeTestCase(NumpyBaseTestCase):
    def setUp(self):
        super(BaseDateTimeTestCase, self).setUp()
        # TODO: remove common client when inserts will be implemented
        self.common_client = self._create_client()

        # Bust tzlocal cache.
        try:
            tzlocal.unix._cache_tz = None
        except AttributeError:
            pass

        try:
            tzlocal.win32._cache_tz = None
        except AttributeError:
            pass

    def tearDown(self):
        self.common_client.disconnect()
        super(BaseDateTimeTestCase, self).tearDown()


class DateTimeTestCase(BaseDateTimeTestCase):
    def test_datetime_type(self):
        query = 'SELECT now()'

        rv = self.client.execute(query, columnar=True)
        self.assertIsInstance(rv[0][0], np.datetime64)

    @require_server_version(20, 1, 2)
    def test_datetime64_type(self):
        query = 'SELECT now64()'

        rv = self.client.execute(query, columnar=True)
        self.assertIsInstance(rv[0][0], np.datetime64)

    def test_simple(self):
        with self.create_table('a Date, b DateTime'):
            data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]
            self.common_client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25\t2012-10-25 14:07:19\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], np.array(['2012-10-25'], dtype='datetime64[D]')
            )
            self.assertArraysEqual(
                inserted[1],
                np.array(['2012-10-25T14:07:19'], dtype='datetime64[ns]')
            )

    def test_handle_errors_from_tzlocal(self):
        with patch('tzlocal.get_localzone') as mocked_get_localzone:
            mocked_get_localzone.side_effect = UnknownTimeZoneError()
            self.client.execute('SELECT now()')

    @require_server_version(20, 1, 2)
    def test_datetime64_frac_trunc(self):
        with self.create_table('a DateTime64'):
            data = [(datetime(2012, 10, 25, 14, 7, 19, 125600), )]
            self.common_client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25 14:07:19.125\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0],
                np.array(['2012-10-25T14:07:19.125'], dtype='datetime64[ns]')
            )

    @require_server_version(20, 1, 2)
    def test_datetime64_explicit_frac(self):
        with self.create_table('a DateTime64(1)'):
            data = [(datetime(2012, 10, 25, 14, 7, 19, 125600),)]
            self.common_client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25 14:07:19.1\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0],
                np.array(['2012-10-25T14:07:19.1'], dtype='datetime64[ns]')
            )


class DateTimeTimezonesTestCase(BaseDateTimeTestCase):
    dt_type = 'DateTime'

    def make_tz_numpy_array(self, dt, tz_name):
        return pd.to_datetime(np.array([dt] * 2, dtype='datetime64[ns]')) \
            .tz_localize(tz_name).to_numpy()

    @contextmanager
    def patch_env_tz(self, tz_name):
        # Although in many cases, changing the TZ environment variable may
        # affect the output of functions like localtime() without calling
        # tzset(), this behavior should not be relied on.
        # https://docs.python.org/3/library/time.html#time.tzset
        with patch.dict(os.environ, {'TZ': tz_name}):
            tzset()
            yield

        tzset()

    # Asia/Kamchatka = UTC+12
    # Asia/Novosibirsk = UTC+7
    # Europe/Moscow = UTC+3

    # 1500010800 second since epoch in Europe/Moscow.
    # 1500000000 second since epoch in UTC.
    dt = datetime(2017, 7, 14, 5, 40)
    dt_str = '2017-07-14T05:40:00'
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
        server_tz_name = self.common_client.execute('SELECT timezone()')[0][0]
        offset = timezone(server_tz_name).utcoffset(self.dt).total_seconds()
        timestamp = 1500010800 - int(offset)

        with self.patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.common_client.execute(
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

                inserted = self.client.execute(query, columnar=True)
                self.assertArraysEqual(
                    inserted[0],
                    np.array([self.dt_str] * 2, dtype='datetime64[ns]')
                )

    def test_use_client_timezone(self):
        # Insert datetime with timezone UTC
        # into column with no timezone
        # using client's timezone Asia/Novosibirsk

        settings = {'use_client_time_zone': True}

        with self.patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.common_client.execute(
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

                inserted = self.client.execute(query, columnar=True,
                                               settings=settings)
                self.assertArraysEqual(
                    inserted[0],
                    np.array([self.dt_str] * 2, dtype='datetime64[ns]')
                )

    # def test_insert_integers(self):
    #     settings = {'use_client_time_zone': True}
    #
    #     with self.patch_env_tz('Europe/Moscow'):
    #         with self.create_table(self.table_columns()):
    #             self.client.execute(
    #                 'INSERT INTO test (a) VALUES', [(1530211034, )],
    #                 settings=settings
    #             )
    #
    #             query = 'SELECT toUInt32(a), a FROM test'
    #             inserted = self.emit_cli(query, use_client_time_zone=1)
    #             self.assertEqual(inserted,
    #             '1530211034\t2018-06-28 21:37:14\n')
    #
    # def test_insert_integer_bounds(self):
    #     with self.create_table('a DateTime'):
    #         self.client.execute(
    #             'INSERT INTO test (a) VALUES',
    #             [(0, ), (1, ), (1500000000, ), (2**32-1, )]
    #         )
    #
    #         query = 'SELECT toUInt32(a) FROM test ORDER BY a'
    #         inserted = self.emit_cli(query)
    #         self.assertEqual(inserted, '0\n1\n1500000000\n4294967295\n')

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_use_server_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with no timezone
        # using server's timezone (Europe/Moscow)

        server_tz_name = self.client.execute('SELECT timezone()')[0][0]
        offset = timezone(server_tz_name).utcoffset(self.dt)

        with self.patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.common_client.execute(
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

                inserted = self.client.execute(query, columnar=True)
                self.assertArraysEqual(
                    inserted[0],
                    np.array([dt.isoformat()] * 2, dtype='datetime64[ns]')
                )

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_use_client_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with no timezone
        # using client's timezone Asia/Novosibirsk

        settings = {'use_client_time_zone': True}

        with self.patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.common_client.execute(
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

                inserted = self.client.execute(query, columnar=True,
                                               settings=settings)
                dt = datetime(2017, 7, 14, 0, 40)
                self.assertArraysEqual(
                    inserted[0],
                    np.array([dt.isoformat()] * 2, dtype='datetime64[ns]')
                )

    @require_server_version(1, 1, 54337)
    def test_column_use_server_timezone(self):
        # Insert datetime with no timezone
        # into column with timezone Asia/Novosibirsk
        # using server's timezone (Europe/Moscow)

        with self.patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.common_client.execute(
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

                inserted = self.client.execute(query, columnar=True)
                self.assertArraysEqual(
                    inserted[0],
                    self.make_tz_numpy_array(self.dt, self.col_tz_name)
                )

    @require_server_version(1, 1, 54337)
    def test_column_use_client_timezone(self):
        # Insert datetime with no timezone
        # into column with timezone Asia/Novosibirsk
        # using client's timezone Europe/Moscow

        settings = {'use_client_time_zone': True}

        with self.patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.common_client.execute(
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

                inserted = self.client.execute(query, columnar=True,
                                               settings=settings)
                self.assertArraysEqual(
                    inserted[0],
                    self.make_tz_numpy_array(self.dt, self.col_tz_name)
                )

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_column_use_server_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with timezone Asia/Novosibirsk
        # using server's timezone (Europe/Moscow)

        with self.patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.common_client.execute(
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

                inserted = self.client.execute(query, columnar=True)
                dt = datetime(2017, 7, 14, 0, 40)
                self.assertArraysEqual(
                    inserted[0], self.make_tz_numpy_array(dt, self.col_tz_name)
                )

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_column_use_client_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with timezone Asia/Novosibirsk
        # using client's timezone (Europe/Moscow)

        settings = {'use_client_time_zone': True}

        with self.patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.common_client.execute(
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

                inserted = self.client.execute(query, columnar=True,
                                               settings=settings)
                dt = datetime(2017, 7, 14, 0, 40)
                self.assertArraysEqual(
                    inserted[0], self.make_tz_numpy_array(dt, self.col_tz_name)
                )


class DateTime64TimezonesTestCase(DateTimeTimezonesTestCase):
    dt_type = 'DateTime64'
    required_server_version = (20, 1, 2)

    def table_columns(self, with_tz=False):
        if not with_tz:
            return 'a {}(0)'.format(self.dt_type)

        return "a {}(0, '{}')".format(self.dt_type, self.col_tz_name)
