from uuid import UUID

from tests.testcase import BaseTestCase
from clickhouse_driver import errors
from tests.util import require_server_version


class ArrayTestCase(BaseTestCase):
    def test_empty(self):
        columns = 'a Array(Int32)'

        data = [([], )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '[]\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_simple(self):
        columns = 'a Array(Int32)'
        data = [([100, 500], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '[100,500]\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_write_column_as_nested_array(self):
        columns = 'a Array(Int32)'
        data = [([100, 500], ), ([100, 500], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '[100,500]\n[100,500]\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nested_with_enum(self):
        columns = "a Array(Array(Enum8('hello' = -1, 'world' = 2)))"

        data = [([['hello', 'world'], ['hello']], )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, "[['hello','world'],['hello']]\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nested_of_nested(self):
        columns = 'a Array(Array(Array(Int32))), b Array(Array(Array(Int32)))'
        data = [([
            [[255, 170], [127, 127, 127, 127, 127], [170, 170, 170], [170]],
            [[255, 255, 255], [255]], [[255], [255], [255]]
        ], [
            [[255, 170], [127, 127, 127, 127, 127], [170, 170, 170], [170]],
            [[255, 255, 255], [255]], [[255], [255], [255]]
        ])]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '[[[255,170],[127,127,127,127,127],[170,170,170],[170]],'
                '[[255,255,255],[255]],[[255],[255],[255]]]\t'
                '[[[255,170],[127,127,127,127,127],[170,170,170],[170]],'
                '[[255,255,255],[255]],[[255],[255],[255]]]\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_multidimensional(self):
        columns = "a Array(Array(Array(Nullable(String))))"
        data = [([[['str1_1', 'str1_2', None], [None]],
                  [['str1_3', 'str1_4', None], [None]]], ),
                ([[['str2_1', 'str2_2', None], [None]]], ),
                ([[['str3_1', 'str3_2', None], [None]]],)]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '[[[\'str1_1\',\'str1_2\',NULL],[NULL]],'
                '[[\'str1_3\',\'str1_4\',NULL],[NULL]]]\n'
                '[[[\'str2_1\',\'str2_2\',NULL],[NULL]]]\n'
                '[[[\'str3_1\',\'str3_2\',NULL],[NULL]]]\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_empty_nested(self):
        columns = "a Array(Array(Array(Int32))), b Array(Array(Array(Int32)))"
        data = [
            ([], [[]],),
        ]

        with self.create_table(columns):
            self.client.execute("INSERT INTO test (a, b) VALUES", data)

            query = "SELECT * FROM test"
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, "[]\t[[]]\n",
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_type_mismatch_error(self):
        columns = 'a Array(Int32)'
        data = [('test', )]

        with self.create_table(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute('INSERT INTO test (a) VALUES', data)

        data = [(['test'], )]

        with self.create_table(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute('INSERT INTO test (a) VALUES', data)

    def test_string_array(self):
        columns = 'a Array(String)'
        data = [(['aaa', 'bbb'], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, "['aaa','bbb']\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_string_nullable_array(self):
        columns = 'a Array(Nullable(String))'
        data = [(['aaa', None, 'bbb'], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, "['aaa',NULL,'bbb']\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_uuid_array(self):
        columns = 'a Array(UUID)'
        data = [([
            UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'),
            UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0')
        ], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "['c0fcbba9-0752-44ed-a5d6-4dfb4342b89d',"
                "'2efcead4-ff55-4db5-bdb4-6b36a308d8e0']\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_uuid_nullable_array(self):
        columns = 'a Array(Nullable(UUID))'
        data = [([
            UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'),
            None,
            UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0')
        ], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "['c0fcbba9-0752-44ed-a5d6-4dfb4342b89d',"
                "NULL,"
                "'2efcead4-ff55-4db5-bdb4-6b36a308d8e0']\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    @require_server_version(19, 16, 13)
    def test_tuple_array(self):
        columns = 'a Array(Tuple(Int32))'
        data = [([], )]

        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, "[]\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
