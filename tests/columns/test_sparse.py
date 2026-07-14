from datetime import date
from io import BytesIO
from unittest import TestCase

from tests.testcase import BaseTestCase
from clickhouse_driver import errors
from clickhouse_driver.columns.base import SparseSerialization
from clickhouse_driver.varint import write_varint

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


class FakeColumn(object):
    null_value = 0
    after_read_items = None


class SparseSerializationTestCase(TestCase):
    # Sending sparse blocks is the server's choice; test directly.

    END_OF_GRANULE_FLAG = 1 << 62

    def make_buf(self, group_sizes):
        buf = BytesIO()
        for size in group_sizes:
            write_varint(size, buf)
        buf.seek(0)
        buf.read_one = lambda: ord(buf.read(1))
        return buf

    def test_read_and_apply_sparse(self):
        serialization = SparseSerialization(FakeColumn())

        # Non-default items at positions 3 and 7 of [0,0,5,0,0,0,7,0].
        buf = self.make_buf([2, 3, self.END_OF_GRANULE_FLAG | 1])
        n_items = serialization.read_sparse(8, buf)

        self.assertEqual(n_items, 2)
        self.assertEqual(serialization.sparse_indexes, [3, 7])
        self.assertEqual(serialization.items_total, 9)
        self.assertEqual(
            serialization.apply_sparse([5, 7]),
            [0, 0, 5, 0, 0, 0, 7, 0]
        )

    def test_apply_sparse_with_after_read_items(self):
        column = FakeColumn()
        column.after_read_items = lambda items, nulls_map=None: \
            tuple(x + 100 for x in items)
        serialization = SparseSerialization(column)

        buf = self.make_buf([0, self.END_OF_GRANULE_FLAG | 1])
        n_items = serialization.read_sparse(3, buf)

        self.assertEqual(n_items, 1)
        self.assertEqual(serialization.apply_sparse([105]), [105, 100])
