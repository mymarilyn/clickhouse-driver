import types
from unittest.mock import patch

from clickhouse_driver.errors import ServerException
from tests.testcase import BaseTestCase, file_config
from tests.util import capture_logging
from clickhouse_driver.util.helpers import chunks


class BlocksTestCase(BaseTestCase):

    def test_return_totals_extremes(self):
        rv = self.client.execute(
            'SELECT a, sum(b + a) FROM ('
            'SELECT arrayJoin(range(3)) - 1 AS a,'
            'arrayJoin(range(4)) AS b'
            ') AS t '
            'GROUP BY a WITH TOTALS '
            'ORDER BY a',
            settings={'extremes': 1}
        )
        self.assertEqual(rv, [
            (-1, 2),
            (0, 6),
            (1, 10),

            # TOTALS
            (0, 18),

            # EXTREMES
            (-1, 2),
            (1, 10)
        ])

    def test_columnar_result(self):
        rv = self.client.execute(
            'SELECT a, sum(b + a) FROM ('
            'SELECT arrayJoin(range(3)) - 1 AS a,'
            'arrayJoin(range(4)) AS b'
            ') AS t '
            'GROUP BY a '
            'ORDER BY a',
            columnar=True
        )
        self.assertEqual(rv, [
            (-1, 0, 1),
            (2, 6, 10)
        ])

    def test_columnar_block_extend(self):
        with self.create_table('a Int32'):
            self.client.execute('INSERT INTO test (a) VALUES', [(1, )])
            self.client.execute('INSERT INTO test (a) VALUES', [(2, )])

            query = 'SELECT * FROM test ORDER BY a'

            inserted = self.client.execute(query, columnar=True)
            self.assertEqual(inserted, [(1, 2)])

    def test_select_with_column_types(self):
        rv = self.client.execute(
            'SELECT CAST(1 AS Int32) AS x', with_column_types=True
        )
        self.assertEqual(rv, ([(1,)], [('x', 'Int32')]))

    def test_select_with_columnar_with_column_types(self):
        progress = self.client.execute_with_progress(
            'SELECT arrayJoin(A) -1 as j,'
            'arrayJoin(A)+1 as k FROM('
            'SELECT range(3) as A)',
            columnar=True, with_column_types=True)
        rv = ([(-1, 0, 1), (1, 2, 3)], [('j', 'Int16'), ('k', 'UInt16')])
        self.assertEqual(progress.get_result(), rv)

    def test_close_connection_on_keyboard_interrupt(self):
        connection = self.client.connection
        with self.assertRaises(KeyboardInterrupt):
            with patch.object(connection, 'send_query') as mocked_send_query:
                mocked_send_query.side_effect = KeyboardInterrupt
                self.client.execute('SELECT 1')

        self.assertFalse(self.client.connection.connected)


class ProgressTestCase(BaseTestCase):
    def test_select_with_progress(self):
        progress = self.client.execute_with_progress('SELECT 2')
        self.assertEqual(
            list(progress),
            [(1, 0), (1, 0)] if self.server_version > (20,) else [(1, 0)]
        )
        self.assertEqual(progress.get_result(), [(2,)])
        self.assertTrue(self.client.connection.connected)

    def test_progress_totals(self):
        progress = self.client.execute_with_progress('SELECT 2')
        self.assertEqual(progress.progress_totals.rows, 0)
        self.assertEqual(progress.progress_totals.bytes, 0)
        self.assertEqual(progress.progress_totals.total_rows, 0)

        self.assertEqual(progress.get_result(), [(2,)])

        self.assertEqual(progress.progress_totals.rows, 1)
        self.assertEqual(progress.progress_totals.bytes, 1)
        self.assertEqual(progress.progress_totals.total_rows, 0)

    def test_select_with_progress_error(self):
        with self.assertRaises(ServerException):
            progress = self.client.execute_with_progress('SELECT error')
            list(progress)
        self.assertFalse(self.client.connection.connected)

    def test_select_with_progress_no_progress_unwind(self):
        progress = self.client.execute_with_progress('SELECT 2')
        self.assertEqual(progress.get_result(), [(2,)])
        self.assertTrue(self.client.connection.connected)

    def test_select_with_progress_cancel(self):
        self.client.execute_with_progress('SELECT 2')
        rv = self.client.cancel()
        self.assertEqual(rv, [(2,)])
        self.assertTrue(self.client.connection.connected)

    def test_select_with_progress_cancel_with_column_types(self):
        self.client.execute_with_progress('SELECT CAST(2 AS Int32) as x')
        rv = self.client.cancel(with_column_types=True)
        self.assertEqual(rv, ([(2,)], [('x', 'Int32')]))
        self.assertTrue(self.client.connection.connected)

    def test_select_with_progress_with_params(self):
        progress = self.client.execute_with_progress(
            'SELECT %(x)s', params={'x': 2}
        )
        self.assertEqual(progress.get_result(), [(2,)])
        self.assertTrue(self.client.connection.connected)

    def test_close_connection_on_keyboard_interrupt(self):
        connection = self.client.connection
        with self.assertRaises(KeyboardInterrupt):
            with patch.object(connection, 'send_query') as mocked_send_query:
                mocked_send_query.side_effect = KeyboardInterrupt
                self.client.execute_with_progress('SELECT 1')

        self.assertFalse(self.client.connection.connected)


class IteratorTestCase(BaseTestCase):
    def test_select_with_iter(self):
        result = self.client.execute_iter(
            'SELECT number FROM system.numbers LIMIT 10'
        )
        self.assertIsInstance(result, types.GeneratorType)
        self.assertEqual(list(result), list(zip(range(10))))
        self.assertEqual(list(result), [])

    def test_select_with_chunk_one_iter(self):
        result = self.client.execute_iter(
            'SELECT number FROM system.numbers LIMIT 10',
            chunk_size=1
        )
        self.assertIsInstance(result, types.GeneratorType)
        self.assertEqual(list(result), list(zip(range(10))))
        self.assertEqual(list(result), [])

    def test_select_with_chunk_some_iter(self):
        result = self.client.execute_iter(
            'SELECT number FROM system.numbers LIMIT 10',
            chunk_size=3
        )
        self.assertIsInstance(result, types.GeneratorType)
        self.assertEqual(list(result), list(chunks(zip(range(10)), 3)))
        self.assertEqual(list(result), [])

    def test_select_with_iter_with_column_types(self):
        result = self.client.execute_iter(
            'SELECT CAST(number AS UInt32) as number '
            'FROM system.numbers LIMIT 10',
            with_column_types=True
        )
        self.assertIsInstance(result, types.GeneratorType)

        self.assertEqual(
            list(result),
            [[('number', 'UInt32')]] + list(zip(range(10)))
        )
        self.assertEqual(list(result), [])

    def test_select_with_iter_error(self):
        with self.assertRaises(ServerException):
            result = self.client.execute_iter('SELECT error')

            self.assertIsInstance(result, types.GeneratorType)
            list(result)

        self.assertFalse(self.client.connection.connected)

    def test_close_connection_on_keyboard_interrupt(self):
        connection = self.client.connection
        with self.assertRaises(KeyboardInterrupt):
            with patch.object(connection, 'send_query') as mocked_send_query:
                mocked_send_query.side_effect = KeyboardInterrupt
                self.client.execute_iter('SELECT 1')

        self.assertFalse(self.client.connection.connected)


class LogTestCase(BaseTestCase):
    required_server_version = (18, 12, 13)

    def test_logs(self):
        with capture_logging('clickhouse_driver.log', 'INFO') as buffer:
            settings = {'send_logs_level': 'debug'}
            query = 'SELECT 1'
            self.client.execute(query, settings=settings)
            self.assertIn(query, buffer.getvalue())

    def test_logs_insert(self):
        with capture_logging('clickhouse_driver.log', 'INFO') as buffer:
            with self.create_table('a Int32'):
                settings = {'send_logs_level': 'debug'}

                query = 'INSERT INTO test (a) VALUES'
                self.client.execute(query, [(1, )], settings=settings)
                logs = buffer.getvalue()
                self.assertIn(query, logs)

                if self.server_version > (19, ):
                    self.assertIn('MemoryTracker', logs)

                # Test all packets of INSERT query are consumed.
                rv = self.client.execute('SELECT 1', settings=settings)
                self.assertEqual(rv, [(1, )])

    def test_logs_with_compression(self):
        compression = 'lz4'
        supported_compressions = (
            file_config.get('db', 'compression').split(',')
        )

        if compression not in supported_compressions:
            self.skipTest(
                'Compression {} is not supported'.format(compression)
            )

        with self.created_client(compression='lz4') as client:
            with capture_logging('clickhouse_driver.log', 'INFO') as buffer:
                settings = {'send_logs_level': 'debug'}
                query = 'SELECT 1'
                client.execute(query, settings=settings)
                self.assertIn(query, buffer.getvalue())
