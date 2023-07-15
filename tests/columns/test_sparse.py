from datetime import date

from tests.testcase import BaseTestCase
from clickhouse_driver import errors

ErrorCodes = errors.ErrorCodes


class SparseTestCase(BaseTestCase):
    required_server_version = (22, 1)

    create_table_template = (
        'CREATE TABLE test ({}) '
        'ENGINE = MergeTree '
        'ORDER BY tuple() '
        'SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5'
    )

    def test_int_all_defaults(self):
        columns = 'a Int32'

        data = [(0, ), (0, ), (0, )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '0\n0\n0\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

        data = [(0,)]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '0\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_int_borders_cases(self):
        columns = 'a Int32'

        data = [(1, ), (0, ), (0, ), (1, ), (0, ), (0, ), (1, )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1\n0\n0\n1\n0\n0\n1\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_int_default_last(self):
        columns = 'a Int32'

        data = [(1, ), (0, ), (0, )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1\n0\n0\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_sparse_tuples(self):
        columns = 'a Int32, b Tuple(Int32, Tuple(Int32, Int32))'

        data = [
            (1, (1, (1, 0))),
            (0, (0, (0, 0))),
            (0, (0, (0, 0)))
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '1\t(1,(1,0))\n'
                '0\t(0,(0,0))\n'
                '0\t(0,(0,0))\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_sparse_dates(self):
        columns = 'a Date32'

        data = [
            (date(1970, 1, 1), ),
            (date(1970, 1, 1), ),
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '1970-01-01\n'
                '1970-01-01\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
