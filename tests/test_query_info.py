from contextlib import contextmanager

from clickhouse_driver import errors
from tests.testcase import BaseTestCase


class QueryInfoTestCase(BaseTestCase):

    @property
    def sample_query(self):
        return 'SELECT * FROM test ORDER BY foo DESC LIMIT 5'

    @contextmanager
    def sample_table(self):
        with self.create_table('foo UInt8'):
            self.client.execute('INSERT INTO test (foo) VALUES',
                                [(i,) for i in range(42)])
            self.client.reset_last_query()
            yield

    def test_default_value(self):
        assert self.client.last_query is None

    def test_store_profile_info_after_execute(self):
        with self.sample_table():
            self.client.execute(self.sample_query)

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.profile_info is not None
        assert last_query.profile_info.rows_before_limit == 42

    def test_store_profile_info_after_execute_iter(self):
        with self.sample_table():
            list(self.client.execute_iter(self.sample_query))

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.profile_info is not None
        assert last_query.profile_info.rows_before_limit == 42

    def test_store_profile_info_after_execute_with_progress(self):
        with self.sample_table():
            progress = self.client.execute_with_progress(self.sample_query)
            for num_rows, total_rows in progress:
                pass
            else:
                progress.get_result()

        last_query = self.client.last_query
        assert last_query is not None
        assert last_query.profile_info is not None
        assert last_query.profile_info.rows_before_limit == 42

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
