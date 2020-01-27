import types
from contextlib import contextmanager

from clickhouse_driver import connect
from clickhouse_driver.dbapi import (
    ProgrammingError, InterfaceError, OperationalError
)
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
        with self.created_connection(**kwargs) as connection:
            cursor = connection.cursor()

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
            self.assertIn('Code: 192', str(e.exception))

    def test_exception_executemany(self):
        with self.created_cursor(user='wrong_user') as cursor:
            with self.assertRaises(OperationalError) as e:
                cursor.executemany('INSERT INTO test VALUES', [(0, )])
            self.assertIn('Code: 192', str(e.exception))
            self.assertEqual(cursor.rowcount, -1)

    def test_rowcount_insert_from_select(self):
        with self.created_cursor() as cursor, self.create_table('a UInt8'):
            cursor.execute(
                'INSERT INTO test '
                'SELECT number FROM system.numbers LIMIT 4'
            )
            self.assertEqual(cursor.rowcount, -1)

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
