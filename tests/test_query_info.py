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
        assert self.client.last_query is None

    def test_store_last_query_after_execute(self):
        with self.sample_table():
            self.client.execute(self.sample_query)

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.profile_info is not None
        assert last_query.profile_info.rows_before_limit == 42

        assert last_query.progress is not None
        assert last_query.progress.rows == 42
        assert last_query.progress.bytes == 42
        assert last_query.progress.total_rows == 0

        assert last_query.elapsed >= 0

    def test_last_query_after_execute_iter(self):
        with self.sample_table():
            list(self.client.execute_iter(self.sample_query))

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.profile_info is not None
        assert last_query.profile_info.rows_before_limit == 42

        assert last_query.progress is not None
        assert last_query.progress.rows == 42
        assert last_query.progress.bytes == 42
        assert last_query.progress.total_rows == 0

        assert last_query.elapsed is None

    def test_last_query_after_execute_with_progress(self):
        with self.sample_table():
            progress = self.client.execute_with_progress(self.sample_query)
            list(progress)
            progress.get_result()

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.profile_info is not None
        assert last_query.profile_info.rows_before_limit == 42

        assert last_query.progress is not None
        assert last_query.progress.rows == 42
        assert last_query.progress.bytes == 42
        assert last_query.progress.total_rows == 0

        assert last_query.elapsed is None

    def test_last_query_progress_total_rows(self):
        self.client.execute('SELECT max(number) FROM numbers(10)')

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.profile_info is not None
        assert last_query.profile_info.rows_before_limit == 10

        assert last_query.progress is not None
        assert last_query.progress.rows == 10
        assert last_query.progress.bytes == 80

        current = self.client.connection.server_info.version_tuple()
        total_rows = 10 if current > (19, 4) else 0
        assert last_query.progress.total_rows == total_rows

        assert last_query.elapsed >= 0

    def test_last_query_after_execute_insert(self):
        with self.sample_table():
            self.client.execute('INSERT INTO test (foo) VALUES',
                                [(i,) for i in range(42)])

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.progress is None
        assert last_query.elapsed >= 0

    def test_override_after_subsequent_queries(self):
        query = 'SELECT * FROM test WHERE foo < %(i)s ORDER BY foo LIMIT 5'
        with self.sample_table():
            for i in range(1, 10):
                self.client.execute(query, {'i': i})

                profile_info = self.client.last_query.profile_info
                assert profile_info.rows_before_limit == i

    def test_reset_last_query(self):
        with self.sample_table():
            self.client.execute(self.sample_query)

        assert self.client.last_query is not None
        self.client.reset_last_query()
        assert self.client.last_query is None

    def test_reset_on_query_error(self):
        with self.assertRaises(errors.ServerException):
            self.client.execute('SELECT answer FROM universe')

        assert self.client.last_query is None

    def test_progress_info_increment(self):
        self.client.execute(
            'SELECT x FROM ('
            'SELECT number AS x FROM numbers(100000000)'
            ') ORDER BY x ASC LIMIT 10'
        )

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.progress is not None
        assert last_query.progress.rows > 100000000
        assert last_query.progress.bytes > 800000000

        current = self.client.connection.server_info.version_tuple()
        total_rows = 100000000 if current > (19, 4) else 0
        assert last_query.progress.total_rows == total_rows
