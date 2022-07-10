from datetime import datetime, date
from unittest.mock import patch

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
from tests.util import require_server_version, patch_env_tz


class BaseDateTimeTestCase(NumpyBaseTestCase):
    def make_numpy_d64ns(self, items):
        return np.array(items, dtype='datetime64[ns]')


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
            data = [
                np.array(['2012-10-25'], dtype='datetime64[D]'),
                np.array(['2012-10-25T14:07:19'], dtype='datetime64[ns]')
            ]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25\t2012-10-25 14:07:19\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], np.array(['2012-10-25'], dtype='datetime64[D]')
            )
            self.assertArraysEqual(
                inserted[1], self.make_numpy_d64ns(['2012-10-25T14:07:19'])
            )

    def test_nullable_date(self):
        with self.create_table('a Nullable(Date)'):
            data = [
                np.array([None, date(2012, 10, 25), None, date(2017, 6, 23)],
                         dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '\\N\n2012-10-25\n\\N\n2017-06-23\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, object)

    def test_nullable_datetime(self):
        with self.create_table('a Nullable(DateTime)'):
            data = [
                np.array([
                    None, datetime(2012, 10, 25, 14, 7, 19),
                    None, datetime(2017, 6, 23, 19, 10, 15)
                ], dtype=object)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\\N\n2012-10-25 14:07:19\n\\N\n2017-06-23 19:10:15\n'
            )

            inserted = self.client.execute(query, columnar=True)

            self.assertArraysEqual(inserted[0], data[0])
            self.assertEqual(inserted[0].dtype, object)

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
            data = [self.make_numpy_d64ns(['2012-10-25T14:07:19.125600'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25 14:07:19.125\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], self.make_numpy_d64ns(['2012-10-25T14:07:19.125'])
            )

    @require_server_version(20, 1, 2)
    def test_datetime64_explicit_frac(self):
        with self.create_table('a DateTime64(1)'):
            data = [self.make_numpy_d64ns(['2012-10-25T14:07:19.125600'])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25 14:07:19.1\n')

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(
                inserted[0], self.make_numpy_d64ns(['2012-10-25T14:07:19.1'])
            )

    @require_server_version(20, 1, 2)
    def test_datetime64_nanosecond_precision(self):
        with self.create_table('a DateTime64(8)'):
            data = [self.make_numpy_d64ns([
                '2012-10-25T14:07:19.12345678',
                '2012-10-25T14:07:19.99999999',
            ])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '2012-10-25 14:07:19.12345678\n'
                '2012-10-25 14:07:19.99999999\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])

    @require_server_version(20, 1, 2)
    def test_datetime64_max_precision(self):
        with self.create_table('a DateTime64(9)'):
            data = [self.make_numpy_d64ns([
                '2012-10-25T14:07:19.123456789',
                '2012-10-25T14:07:19.999999999',
            ])]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '2012-10-25 14:07:19.123456789\n'
                '2012-10-25 14:07:19.999999999\n'
            )

            inserted = self.client.execute(query, columnar=True)
            self.assertArraysEqual(inserted[0], data[0])

    def test_insert_integers_datetime(self):
        with self.create_table('a DateTime'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [np.array([1530211034], dtype=np.uint32)], columnar=True
            )

            query = 'SELECT toUInt32(a), a FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1530211034\t2018-06-28 21:37:14\n')

    @require_server_version(20, 1, 2)
    def test_insert_integers_datetime64(self):
        with self.create_table('a DateTime64'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [np.array([1530211034123], dtype=np.uint64)], columnar=True
            )

            query = 'SELECT toUInt32(a), a FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1530211034\t2018-06-28 21:37:14.123\n')

    def test_insert_integer_bounds(self):
        with self.create_table('a DateTime'):
            self.client.execute(
                'INSERT INTO test (a) VALUES',
                [np.array([0, 1, 1500000000, 2**32-1], dtype=np.uint32)],
                columnar=True
            )

            query = 'SELECT toUInt32(a) FROM test ORDER BY a'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '0\n1\n1500000000\n4294967295\n')


class DateTimeTimezonesTestCase(BaseDateTimeTestCase):
    dt_type = 'DateTime'

    def make_tz_numpy_array(self, dt, tz_name):
        dtype = 'datetime64[ns]'

        return pd.to_datetime(np.array([dt] * 2, dtype=dtype)) \
            .tz_localize(tz_name).to_numpy(dtype)

    # Asia/Kamchatka = UTC+12
    # Asia/Novosibirsk = UTC+7
    # Europe/Moscow = UTC+3

    # 1500010800 second since epoch in Europe/Moscow.
    # 1500000000 second since epoch in UTC.
    dt = datetime(2017, 7, 14, 5, 40)
    dt_str = dt.isoformat()

    # properties for lazy evaluation for dealing with AttributeError when no
    # numpy/pandas installed
    @property
    def dt_arr(self):
        return np.array([self.dt_str], dtype='datetime64[s]')

    @property
    def dt_tz(self):
        return pd.to_datetime(self.dt_arr).tz_localize('Asia/Kamchatka')

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
                    'INSERT INTO test (a) VALUES', [self.dt_arr], columnar=True
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
                    inserted[0], self.make_numpy_d64ns([self.dt_str] * 2)
                )

    def test_use_client_timezone(self):
        # Insert datetime with timezone UTC
        # into column with no timezone
        # using client's timezone Asia/Novosibirsk

        settings = {'use_client_time_zone': True}

        with patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [self.dt_arr],
                    settings=settings, columnar=True
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
                    inserted[0], self.make_numpy_d64ns([self.dt_str] * 2)
                )

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
                    'INSERT INTO test (a) VALUES', [self.dt_tz], columnar=True
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

                dt = (self.dt_tz.to_pydatetime()[0].astimezone(utc) + offset) \
                    .replace(tzinfo=None)
                self.assertEqual(inserted, '{dt}\n{dt}\n'.format(dt=dt))

                inserted = self.client.execute(query, columnar=True)
                self.assertArraysEqual(
                    inserted[0], self.make_numpy_d64ns([dt.isoformat()] * 2)
                )

    @require_server_version(1, 1, 54337)
    def test_datetime_with_timezone_use_client_timezone(self):
        # Insert datetime with timezone Asia/Kamchatka
        # into column with no timezone
        # using client's timezone Asia/Novosibirsk

        settings = {'use_client_time_zone': True}

        with patch_env_tz('Asia/Novosibirsk'):
            with self.create_table(self.table_columns()):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [self.dt_tz],
                    settings=settings, columnar=True
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
                    inserted[0], self.make_numpy_d64ns([dt.isoformat()] * 2)
                )

    @require_server_version(1, 1, 54337)
    def test_column_use_server_timezone(self):
        # Insert datetime with no timezone
        # into column with timezone Asia/Novosibirsk
        # using server's timezone (Europe/Moscow)

        with patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [self.dt_arr], columnar=True
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

        with patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [self.dt_arr],
                    settings=settings, columnar=True
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

        with patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [self.dt_tz], columnar=True
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

        with patch_env_tz('Europe/Moscow'):
            with self.create_table(self.table_columns(with_tz=True)):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [self.dt_tz],
                    settings=settings, columnar=True
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
