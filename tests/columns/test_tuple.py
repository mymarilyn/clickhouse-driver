from datetime import date

from clickhouse_driver import errors
from tests.testcase import BaseTestCase
from tests.util import require_server_version


class TupleTestCase(BaseTestCase):
    def entuple(self, lst):
        return tuple(
            self.entuple(x) if isinstance(x, list) else x for x in lst
        )

    def test_simple(self):
        columns = 'a Tuple(Int32, String)'
        data = [((1, 'a'), ), ((2, 'b'), )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, "(1,'a')\n(2,'b')\n")

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_tuple_single_element(self):
        columns = 'a Tuple(Int32)'
        data = [((1, ), ), ((2, ), )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, "(1)\n(2)\n")

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        data = [
            ((1, 'a'),),
            ((2, None),), ((None, None),), ((None, 'd'),),
            ((5, 'e'),)
        ]

        with self.create_table('a Tuple(Nullable(Int32), Nullable(String))'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "(1,'a')\n"
                "(2,NULL)\n(NULL,NULL)\n(NULL,'d')\n"
                "(5,'e')\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nested_tuple_with_common_types(self):
        columns = 'a Tuple(String, Tuple(Int32, String), String)'
        data = [
            (('one', (1, 'a'), 'two'),),
            (('three', (2, 'b'), 'four'),)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "('one',(1,'a'),'two')\n"
                "('three',(2,'b'),'four')\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_tuple_of_tuples(self):
        columns = (
            "a Tuple("
            "Tuple(Int32, String),"
            "Tuple(Enum8('hello' = 1, 'world' = 2), Date)"
            ")"
        )
        data = [
            (((1, 'a'), (1, date(2020, 3, 11))),),
            (((2, 'b'), (2, date(2020, 3, 12))),)
        ]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "((1,'a'),('hello','2020-03-11'))\n"
                "((2,'b'),('world','2020-03-12'))\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (((1, 'a'), ('hello', date(2020, 3, 11))), ),
                    (((2, 'b'), ('world', date(2020, 3, 12))), )
                ]
            )

    def test_tuple_of_arrays(self):
        data = [(([1, 2, 3],),), (([4, 5, 6],),)]

        with self.create_table('a Tuple(Array(Int32))'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "([1,2,3])\n([4,5,6])\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    # Bug in Array of Tuple handing before 19.16.13:
    # DESCRIBE TABLE test
    #
    # | a.1  | Array(UInt8) |
    # | a.2  | Array(UInt8) |
    # | a.3  | Array(UInt8) |
    # https://github.com/ClickHouse/ClickHouse/pull/8866
    @require_server_version(19, 16, 13)
    def test_array_of_tuples(self):
        data = [
            ([(1, 2, 3), (4, 5, 6)],),
            ([(7, 8, 9)],),
        ]

        with self.create_table('a Array(Tuple(UInt8, UInt8, UInt8))'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "[(1,2,3),(4,5,6)]\n"
                "[(7,8,9)]\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_type_mismatch_error(self):
        columns = 'a Tuple(Int32)'
        data = [('test', )]

        with self.create_table(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute('INSERT INTO test (a) VALUES', data)
