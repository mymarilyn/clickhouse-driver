from contextlib import contextmanager

from clickhouse_driver import errors
from tests.testcase import BaseTestCase


class QueryInfoTestCase(BaseTestCase):

    @property
    def sample_query(self):
        return 'SELECT * FROM test GROUP BY foo ORDER BY foo DESC LIMIT 5'

    @contextmanager
    def sample_table(self):
        with self.create_table('foo UInt8'):
            self.client.execute('INSERT INTO test (foo) VALUES',
                                [(i,) for i in range(42)])
            self.client.reset_last_query()
            yield

    def test_default_value(self):
        self.assertIsNone(self.client.last_query)

    def test_store_last_query_after_execute(self):
        with self.sample_table():
            self.client.execute(self.sample_query)

        last_query = self.client.last_query
        self.assertIsNotNone(last_query)
        self.assertIsNotNone(last_query.profile_info)
        self.assertEqual(last_query.profile_info.rows_before_limit, 42)

        self.assertIsNotNone(last_query.progress)
        self.assertEqual(last_query.progress.rows, 42)
        self.assertEqual(last_query.progress.bytes, 42)
        self.assertEqual(last_query.progress.total_rows, 0)
        if self.server_version > (22, 8):
            self.assertGreater(last_query.progress.elapsed_ns, 0)

        self.assertGreater(last_query.elapsed, 0)

        if self.server_version >= (21, 12):
            self.assertEqual(last_query.stats['SelectQuery'], 1)
            self.assertEqual(last_query.stats['SelectedRows'], 42)
        else:
            self.assertDictEqual(last_query.stats, {})

    def test_last_query_after_execute_iter(self):
        with self.sample_table():
            list(self.client.execute_iter(self.sample_query))

        last_query = self.client.last_query
        self.assertIsNotNone(last_query)
        self.assertIsNotNone(last_query.profile_info)
        self.assertEqual(last_query.profile_info.rows_before_limit, 42)

        self.assertIsNotNone(last_query.progress)
        self.assertEqual(last_query.progress.rows, 42)
        self.assertEqual(last_query.progress.bytes, 42)
        self.assertEqual(last_query.progress.total_rows, 0)
        if self.server_version > (22, 8):
            self.assertGreater(last_query.progress.elapsed_ns, 0)

        self.assertEqual(last_query.elapsed, 0)

        if self.server_version >= (21, 12):
            self.assertEqual(last_query.stats['SelectQuery'], 1)
            self.assertEqual(last_query.stats['SelectedRows'], 42)
        else:
            self.assertDictEqual(last_query.stats, {})

    def test_last_query_after_execute_with_progress(self):
        with self.sample_table():
            progress = self.client.execute_with_progress(self.sample_query)
            list(progress)
            progress.get_result()

        last_query = self.client.last_query
        self.assertIsNotNone(last_query)
        self.assertIsNotNone(last_query.profile_info)
        self.assertEqual(last_query.profile_info.rows_before_limit, 42)

        self.assertIsNotNone(last_query.progress)
        self.assertEqual(last_query.progress.rows, 42)
        self.assertEqual(last_query.progress.bytes, 42)
        self.assertEqual(last_query.progress.total_rows, 0)
        if self.server_version > (22, 8):
            self.assertGreater(last_query.progress.elapsed_ns, 0)

        self.assertEqual(last_query.elapsed, 0)

        if self.server_version >= (21, 12):
            self.assertEqual(last_query.stats['SelectQuery'], 1)
            self.assertEqual(last_query.stats['SelectedRows'], 42)
        else:
            self.assertDictEqual(last_query.stats, {})

    def test_last_query_progress_total_rows(self):
        self.client.execute('SELECT number FROM numbers(10) LIMIT 10')

        last_query = self.client.last_query
        self.assertIsNotNone(last_query)
        self.assertIsNotNone(last_query.profile_info)
        self.assertEqual(last_query.profile_info.rows_before_limit, 10)

        self.assertIsNotNone(last_query.progress)
        self.assertEqual(last_query.progress.rows, 10)
        self.assertEqual(last_query.progress.bytes, 80)

        total_rows = 10 if self.server_version > (19, 4) else 0
        self.assertEqual(last_query.progress.total_rows, total_rows)
        if self.server_version > (22, 8):
            self.assertGreater(last_query.progress.elapsed_ns, 0)

        self.assertGreater(last_query.elapsed, 0)

        if self.server_version >= (21, 12):
            self.assertEqual(last_query.stats['SelectQuery'], 1)
            self.assertEqual(last_query.stats['SelectedRows'], 10)
        else:
            self.assertDictEqual(last_query.stats, {})

    def test_last_query_after_execute_insert(self):
        with self.sample_table():
            self.client.execute('INSERT INTO test (foo) VALUES',
                                [(i,) for i in range(42)])

        last_query = self.client.last_query
        self.assertIsNotNone(last_query)
        self.assertIsNotNone(last_query.progress)
        self.assertEqual(last_query.progress.rows, 0)
        self.assertEqual(last_query.progress.bytes, 0)
        if self.server_version > (22, 8):
            self.assertEqual(last_query.progress.elapsed_ns, 0)

        self.assertGreater(last_query.elapsed, 0)

        if self.server_version >= (21, 12):
            self.assertEqual(last_query.stats['InsertQuery'], 1)
            self.assertEqual(last_query.stats['InsertedRows'], 42)
        else:
            self.assertDictEqual(last_query.stats, {})

    def test_override_after_subsequent_queries(self):
        query = 'SELECT * FROM test WHERE foo < %(i)s ORDER BY foo LIMIT 5'
        with self.sample_table():
            for i in range(1, 10):
                self.client.execute(query, {'i': i})

                last_query = self.client.last_query
                self.assertEqual(last_query.profile_info.rows_before_limit, i)

                if self.server_version >= (21, 12):
                    self.assertEqual(last_query.stats['SelectQuery'], 1)
                    self.assertEqual(last_query.stats['SelectedRows'], 42)
                else:
                    self.assertDictEqual(last_query.stats, {})

    def test_reset_last_query(self):
        with self.sample_table():
            self.client.execute(self.sample_query)

        self.assertIsNotNone(self.client.last_query)
        self.client.reset_last_query()
        self.assertIsNone(self.client.last_query)

    def test_reset_on_query_error(self):
        with self.assertRaises(errors.ServerException):
            self.client.execute('SELECT answer FROM universe')

        self.assertIsNone(self.client.last_query)

    def test_progress_info_increment(self):
        self.client.execute(
            'SELECT x FROM ('
            'SELECT number AS x FROM numbers(100000000)'
            ') ORDER BY x ASC LIMIT 10'
        )

        last_query = self.client.last_query
        self.assertIsNotNone(last_query)
        self.assertIsNotNone(last_query.progress)
        self.assertGreaterEqual(last_query.progress.rows, 100000000)
        self.assertGreaterEqual(last_query.progress.bytes, 800000000)

        total_rows = 100000000 if self.server_version > (19, 4) else 0
        self.assertEqual(last_query.progress.total_rows, total_rows)

        last_query = self.client.last_query
        if self.server_version >= (21, 12):
            self.assertEqual(last_query.stats['SelectQuery'], 1)
            self.assertEqual(last_query.stats['SelectedRows'], 100000000)
            self.assertEqual(last_query.stats['SelectedBytes'], 800000000)
        else:
            self.assertDictEqual(last_query.stats, {})

    def test_progress_info_ddl(self):
        self.client.execute('DROP TABLE IF EXISTS foo')

        last_query = self.client.last_query
        self.assertIsNotNone(last_query)
        self.assertIsNotNone(last_query.progress)
        self.assertEqual(last_query.progress.rows, 0)
        self.assertEqual(last_query.progress.bytes, 0)
        if self.server_version > (22, 8):
            self.assertEqual(last_query.progress.elapsed_ns, 0)

        self.assertGreater(last_query.elapsed, 0)
        self.assertDictEqual(last_query.stats, {})
