from datetime import date

from tests.testcase import BaseTestCase
from clickhouse_driver import errors
from clickhouse_driver.errors import ServerException


class InsertTestCase(BaseTestCase):
    def test_type_mismatch(self):
        with self.create_table('a Float32'):
            with self.assertRaises(errors.TypeMismatchError):
                data = [(date(2012, 10, 25), )]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_no_such_column(self):
        with self.create_table('a Float32'):
            with self.assertRaises(ServerException):
                data = [(1, )]
                self.client.execute(
                    'INSERT INTO test (b) VALUES', data
                )

    def test_data_malformed_rows(self):
        with self.create_table('a Int8'), self.assertRaises(TypeError):
            data = [1]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

    def test_data_less_columns_then_expected(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, )]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Expected 2 columns, got 1')

    def test_data_more_columns_then_expected(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2, 3)]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Expected 2 columns, got 3')

    def test_data_different_rows_length(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(ValueError) as e:
                data = [(1, 2), (3, )]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )
            self.assertEqual(str(e.exception), 'Different rows length')

    def test_data_different_rows_length_from_dicts(self):
        with self.create_table('a Int8, b Int8'):
            with self.assertRaises(KeyError):
                data = [{'a': 1, 'b': 2}, {'a': 3}]
                self.client.execute(
                    'INSERT INTO test (a, b) VALUES', data
                )

    def test_data_unsupported_row_type(self):
        with self.create_table('a Int8'):
            with self.assertRaises(TypeError) as e:
                data = [1]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )
            self.assertIn('dict, list or tuple is expected', str(e.exception))

    def test_data_dicts_ok(self):
        with self.create_table('a Int8, b Int8'):
            data = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1\t2\n3\t4\n')
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(1, 2), (3, 4)])

    def test_data_dicts_mixed_with_lists(self):
        with self.create_table('a Int8'):
            with self.assertRaises(TypeError) as e:
                data = [{'a': 1}, (2, )]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

            self.assertIn('dict is expected', str(e.exception))

            with self.assertRaises(TypeError) as e:
                data = [(1, ), {'a': 2}]
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

            self.assertIn('list or tuple is expected', str(e.exception))

    def test_empty_insert(self):
        with self.create_table('a Int8'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', []
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '')
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [])
