import types
from collections import namedtuple
from contextlib import contextmanager
import socket
from unittest.mock import patch

from clickhouse_driver import connect
from clickhouse_driver.dbapi import (
    ProgrammingError, InterfaceError, OperationalError
)
from clickhouse_driver.dbapi.extras import DictCursor, NamedTupleCursor
from tests.testcase import BaseTestCase


class DBAPITestCaseBase(BaseTestCase):
    def create_connection(self, **kwargs):
        kwargs.setdefault('user', self.user)
        kwargs.setdefault('password', self.password)
        kwargs.setdefault('host', self.host)
        kwargs.setdefault('port', self.port)
        kwargs.setdefault('database', self.database)

        return connect(**kwargs)

    @contextmanager
    def created_connection(self, **kwargs):
        connection = self.create_connection(**kwargs)
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def created_cursor(self, **kwargs):
        cursor_kwargs = kwargs.pop('cursor_kwargs', {})
        with self.created_connection(**kwargs) as connection:
            cursor = connection.cursor(**cursor_kwargs)

            try:
                yield cursor
            finally:
                cursor.close()


class DBAPITestCase(DBAPITestCaseBase):
    def test_no_host_and_dsn(self):
        with self.assertRaises(ValueError) as e:
            self.create_connection(host=None)
            self.assertEqual(str(e.exception), 'host or dsn is required')

    def test_simple(self):
        with self.created_connection() as connection:
            cursor = connection.cursor()
            rv = cursor.execute('SELECT 1')
            self.assertIsNone(rv)
            self.assertEqual(cursor.fetchall(), [(1, )])

    def test_from_dsn(self):
        connection = connect(
            'clickhouse://{user}:{password}@{host}:{port}/{database}'.format(
                user=self.user, password=self.password,
                host=self.host, port=self.port, database=self.database
            )
        )
        cursor = connection.cursor()
        rv = cursor.execute('SELECT 1')
        self.assertIsNone(rv)
        self.assertEqual(cursor.fetchall(), [(1, )])
        connection.close()

    def test_connect_default_params(self):
        connection = connect(host=self.host)
        cursor = connection.cursor()

        rv = cursor.execute('SELECT 1')
        self.assertIsNone(rv)
        self.assertEqual(cursor.fetchall(), [(1, )])
        connection.close()

    def test_execute_fetchone(self):
        with self.created_cursor() as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, list)

            self.assertEqual(cursor.fetchone(), (0, ))
            self.assertEqual(cursor.fetchone(), (1, ))
            self.assertEqual(cursor.fetchone(), (2, ))
            self.assertEqual(cursor.fetchone(), (3, ))
            self.assertEqual(cursor.fetchone(), None)

    def test_execute_fetchmany(self):
        with self.created_cursor() as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, list)
            self.assertEqual(cursor.fetchmany(), [(0, )])
            self.assertEqual(cursor.fetchmany(None), [(1, )])
            self.assertEqual(cursor.fetchmany(0), [])
            self.assertEqual(cursor.fetchmany(-1), [(2, ), (3, )])

            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.fetchmany(1), [(0, )])
            self.assertEqual(cursor.fetchmany(2), [(1, ), (2, )])
            self.assertEqual(cursor.fetchmany(3), [(3, )])
            self.assertEqual(cursor.fetchmany(3), [])

            cursor.arraysize = 2
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.fetchmany(), [(0,), (1,)])
            self.assertEqual(cursor.fetchmany(), [(2,), (3,)])

    def test_execute_fetchall(self):
        with self.created_cursor() as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.rowcount, 4)
            self.assertEqual(cursor.fetchall(), [(0,), (1,), (2,), (3,)])

    def test_executemany(self):
        with self.created_cursor() as cursor, self.create_table('a UInt32'):
            data = [(0, ), (1, ), (2, )]
            rv = cursor.executemany('INSERT INTO test VALUES', data)
            self.assertIsNone(rv, None)
            self.assertEqual(cursor.rowcount, 3)

            cursor.execute('SELECT * FROM test')
            self.assertEqual(cursor.fetchall(), data)

    def test_fake_transactions(self):
        with self.created_connection() as connection:
            connection.commit()
            connection.rollback()

    def test_exception_execute(self):
        with self.created_cursor(user='wrong_user') as cursor:
            with self.assertRaises(OperationalError) as e:
                cursor.execute('SELECT 1')
            code = 516 if self.server_version > (20, ) else 192
            self.assertIn('Code: {}'.format(code), str(e.exception))

    def test_exception_executemany(self):
        with self.created_cursor(user='wrong_user') as cursor:
            with self.assertRaises(OperationalError) as e:
                cursor.executemany('INSERT INTO test VALUES', [(0, )])
            code = 516 if self.server_version > (20, ) else 192
            self.assertIn('Code: {}'.format(code), str(e.exception))
            self.assertEqual(cursor.rowcount, -1)

    def test_rowcount_insert_from_select(self):
        with self.created_cursor() as cursor, self.create_table('a UInt8'):
            cursor.execute(
                'INSERT INTO test '
                'SELECT number FROM system.numbers LIMIT 4'
            )
            self.assertEqual(cursor.rowcount, -1)

    def test_execute_insert(self):
        with self.created_cursor() as cursor, self.create_table('a UInt8'):
            cursor.execute('INSERT INTO test VALUES', [[4]])
            self.assertEqual(cursor.rowcount, 1)

    def test_description(self):
        with self.created_cursor() as cursor:
            self.assertIsNone(cursor.description)
            cursor.execute('SELECT CAST(1 AS UInt32) AS test')
            desc = cursor.description
            self.assertEqual(len(desc), 1)
            self.assertEqual(desc[0].name, 'test')
            self.assertEqual(desc[0].type_code, 'UInt32')

    def test_pep249_sizes(self):
        with self.created_cursor() as cursor:
            cursor.setinputsizes(0)
            cursor.setoutputsize(0)

    def test_ddl(self):
        with self.created_cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS test')
            self.assertEqual(cursor.fetchall(), [])
            self.assertEqual(cursor.rowcount, -1)

    def test_remember_last_host(self):
        self.n_calls = 0
        getaddrinfo = socket.getaddrinfo

        def side_getaddrinfo(host, *args, **kwargs):
            if host == 'wrong_host':
                self.n_calls += 1
                raise socket.error(-2, 'Name or service not known')
            return getaddrinfo(host, *args, **kwargs)

        with patch('socket.getaddrinfo') as mocked_getaddrinfo:
            mocked_getaddrinfo.side_effect = side_getaddrinfo

            conn_kwargs = {
                'host': 'wrong_host',
                'port': 1234,
                'alt_hosts': '{}:{}'.format(self.host, self.port)
            }
            with self.created_connection(**conn_kwargs) as connection:
                cursor = connection.cursor()
                cursor.execute('SELECT 1')
                self.assertEqual(cursor.fetchall(), [(1, )])
                cursor.close()

                cursor = connection.cursor()
                cursor.execute('SELECT 1')
                self.assertEqual(cursor.fetchall(), [(1, )])
                cursor.close()

        # Last host must be remembered and getaddrinfo must call exactly
        # once with host == 'wrong_host'.
        self.assertEqual(self.n_calls, 1)

    def test_remove_cursor_from_connection_on_closing(self):
        with self.created_connection() as connection:
            self.assertEqual(len(connection.cursors), 0)
            cur = connection.cursor()
            self.assertEqual(len(connection.cursors), 1)
            cur.close()
            self.assertEqual(len(connection.cursors), 0)


class StreamingTestCase(DBAPITestCaseBase):
    def test_fetchone(self):
        with self.created_cursor() as cursor:
            cursor.set_stream_results(True, 2)
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, types.GeneratorType)

            self.assertEqual(cursor.fetchone(), (0, ))
            self.assertEqual(cursor.fetchone(), (1, ))
            self.assertEqual(cursor.fetchone(), (2, ))
            self.assertEqual(cursor.fetchone(), (3, ))
            self.assertEqual(cursor.fetchone(), None)

    def test_fetchmany(self):
        with self.created_cursor() as cursor:
            cursor.set_stream_results(True, 2)
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, types.GeneratorType)
            self.assertEqual(cursor.fetchmany(), [(0, )])
            self.assertEqual(cursor.fetchmany(None), [(1, )])
            self.assertEqual(cursor.fetchmany(0), [])
            self.assertEqual(cursor.fetchmany(-1), [(2, ), (3, )])

            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.fetchmany(1), [(0, )])
            self.assertEqual(cursor.fetchmany(2), [(1, ), (2, )])
            self.assertEqual(cursor.fetchmany(3), [(3, )])
            self.assertEqual(cursor.fetchmany(3), [])

    def test_fetchall(self):
        with self.created_cursor() as cursor:
            cursor.set_stream_results(True, 2)

            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            # Check rowcount before and after fetch.
            self.assertEqual(cursor.rowcount, -1)
            self.assertEqual(cursor.fetchall(), [(0, ), (1, ), (2, ), (3, )])
            self.assertEqual(cursor.rowcount, -1)


class ReprTestCase(DBAPITestCaseBase):
    def test_cursor_repr(self):
        with self.created_cursor() as cursor:
            self.assertIn('closed: False', str(cursor))

    def test_connection_repr(self):
        with self.created_connection() as conn:
            self.assertIn('closed: False', str(conn))


class ExtrasTestCase(DBAPITestCaseBase):
    def test_columns_with_types_select(self):
        with self.created_cursor() as cursor:
            self.assertIsNone(cursor.columns_with_types)
            cursor.execute(
                'SELECT CAST(number AS UInt64) AS x '
                'FROM system.numbers LIMIT 4'
            )
            cursor.fetchall()
            self.assertEqual(cursor.columns_with_types, [('x', 'UInt64')])

    def test_columns_with_types_insert(self):
        with self.created_cursor() as cursor, self.create_table('a UInt8'):
            cursor.executemany('INSERT INTO test (a) VALUES', [(123, )])
            self.assertIsNone(cursor.columns_with_types)

    def test_columns_with_types_streaming(self):
        with self.created_cursor() as cursor:
            cursor.set_stream_results(True, 2)
            cursor.execute(
                'SELECT CAST(number AS UInt64) AS x '
                'FROM system.numbers LIMIT 4'
            )
            self.assertEqual(cursor.columns_with_types, [('x', 'UInt64')])
            list(cursor)
            self.assertEqual(cursor.columns_with_types, [('x', 'UInt64')])

    def test_set_external_tables(self):
        with self.created_cursor() as cursor:
            data = [(0, ), (1, ), (2, )]
            cursor.set_external_table('table1', [('x', 'UInt32')], data)
            cursor.execute('SELECT * FROM table1')
            self.assertEqual(cursor.fetchall(), data)

    def test_settings(self):
        with self.created_cursor() as cursor:
            cursor.set_settings({'max_threads': 100500})

            cursor.execute(
                "SELECT name, value, changed FROM system.settings "
                "WHERE name = 'max_threads'",
            )
            self.assertEqual(cursor.fetchall(), [('max_threads', '100500', 1)])

    def test_set_query_id(self):
        with self.created_cursor() as cursor:
            query_id = 'my_query_id'
            cursor.set_query_id(query_id)
            cursor.execute(
                'SELECT query_id '
                'FROM system.processes '
                'WHERE query_id = %(query_id)s',
                {'query_id': query_id}
            )
            self.assertEqual(cursor.fetchall(), [(query_id, )])

    def test_types_check(self):
        with self.created_cursor() as cursor, self.create_table('a UInt8'):
            cursor.set_types_check(True)

            data = [(300, )]
            cursor.executemany('INSERT INTO test (a) VALUES', data)
            cursor.execute('SELECT * FROM test')
            self.assertEqual(cursor.fetchall(), [(44, )])

    def test_cursor_iteration(self):
        with self.created_cursor() as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(list(cursor), [(0,), (1,), (2,), (3,)])

    def test_context_managers(self):
        with self.create_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT 1')
                self.assertEqual(cursor.fetchall(), [(1, )])


class InterfaceTestCase(DBAPITestCaseBase):
    def test_execute_after_close(self):
        with self.created_cursor() as cursor:
            cursor.close()
            with self.assertRaises(InterfaceError) as e:
                cursor.execute('SELECT 1')
            self.assertEqual(str(e.exception), 'cursor already closed')

    def test_create_cursor_on_closed_connection(self):
        connection = self.create_connection()
        connection.close()

        with self.assertRaises(InterfaceError) as e:
            connection.cursor()
            self.assertEqual(str(e.exception), 'connection already closed')

    def test_execute_fetch_before_query(self):
        with self.created_cursor() as cursor:
            with self.assertRaises(ProgrammingError) as e:
                cursor.fetchall()
            self.assertEqual(str(e.exception), 'no results to fetch')


class DictCursorFactoryTestCase(DBAPITestCaseBase):
    def test_execute_fetchone(self):
        cursor_kwargs = {'cursor_factory': DictCursor}

        with self.created_cursor(cursor_kwargs=cursor_kwargs) as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, list)
            self.assertEqual(cursor.fetchone(), {'number': 0})
            self.assertEqual(cursor.fetchone(), {'number': 1})
            self.assertEqual(cursor.fetchone(), {'number': 2})
            self.assertEqual(cursor.fetchone(), {'number': 3})
            self.assertEqual(cursor.fetchone(), None)

    def test_execute_fetchmany(self):
        cursor_kwargs = {'cursor_factory': DictCursor}

        with self.created_cursor(cursor_kwargs=cursor_kwargs) as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, list)
            self.assertEqual(cursor.fetchmany(), [{'number': 0}])
            self.assertEqual(cursor.fetchmany(None), [{'number': 1}])
            self.assertEqual(cursor.fetchmany(0), [])
            self.assertEqual(
                cursor.fetchmany(-1), [{'number': 2}, {'number': 3}]
            )

            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.fetchmany(1), [{'number': 0}])
            self.assertEqual(
                cursor.fetchmany(2), [{'number': 1}, {'number': 2}]
            )
            self.assertEqual(cursor.fetchmany(3), [{'number': 3}])
            self.assertEqual(cursor.fetchmany(3), [])

            cursor.arraysize = 2
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(
                cursor.fetchmany(), [{'number': 0}, {'number': 1}])
            self.assertEqual(
                cursor.fetchmany(), [{'number': 2}, {'number': 3}]
            )

    def test_execute_fetchall(self):
        cursor_kwargs = {'cursor_factory': DictCursor}

        with self.created_cursor(cursor_kwargs=cursor_kwargs) as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.rowcount, 4)
            self.assertEqual(
                cursor.fetchall(), [
                    {'number': 0}, {'number': 1}, {'number': 2}, {'number': 3}
                ])


class NamedTupleCursorFactoryTestCase(DBAPITestCaseBase):
    def test_execute_fetchone(self):
        cursor_kwargs = {'cursor_factory': NamedTupleCursor}

        with self.created_cursor(cursor_kwargs=cursor_kwargs) as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, list)
            nt = namedtuple('Record', cursor._columns)

            self.assertEqual(cursor.fetchone(), nt(0))
            self.assertEqual(cursor.fetchone(), nt(1))
            self.assertEqual(cursor.fetchone(), nt(2))
            self.assertEqual(cursor.fetchone(), nt(3))
            self.assertEqual(cursor.fetchone(), None)

    def test_execute_fetchmany(self):
        cursor_kwargs = {'cursor_factory': NamedTupleCursor}

        with self.created_cursor(cursor_kwargs=cursor_kwargs) as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, list)
            nt = namedtuple('Record', cursor._columns)

            self.assertEqual(cursor.fetchmany(), [nt(0)])
            self.assertEqual(cursor.fetchmany(None), [nt(1)])
            self.assertEqual(cursor.fetchmany(0), [])
            self.assertEqual(cursor.fetchmany(-1), [nt(2), nt(3)])

            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.fetchmany(1), [nt(0)])
            self.assertEqual(cursor.fetchmany(2), [nt(1), nt(2)])
            self.assertEqual(cursor.fetchmany(3), [nt(3)])
            self.assertEqual(cursor.fetchmany(3), [])

            cursor.arraysize = 2
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.fetchmany(), [nt(0), nt(1)])
            self.assertEqual(cursor.fetchmany(), [nt(2), nt(3)])

    def test_execute_fetchall(self):
        cursor_kwargs = {'cursor_factory': NamedTupleCursor}

        with self.created_cursor(cursor_kwargs=cursor_kwargs) as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')
            self.assertEqual(cursor.rowcount, 4)
            nt = namedtuple('Record', cursor._columns)

            self.assertEqual(cursor.fetchall(), [nt(0), nt(1), nt(2), nt(3)])

    def test_make_nt_caching(self):
        cursor_kwargs = {'cursor_factory': NamedTupleCursor}

        with self.created_cursor(cursor_kwargs=cursor_kwargs) as cursor:
            cursor.execute('SELECT number FROM system.numbers LIMIT 4')

            self.assertIsInstance(cursor._rows, list)
            nt = namedtuple('Record', cursor._columns)

            self.assertEqual(cursor.fetchone(), nt(0))

            with patch('clickhouse_driver.dbapi.extras.namedtuple') as nt_mock:
                nt_mock.side_effect = ValueError
                self.assertEqual(cursor.fetchone(), nt(1))
