from datetime import date

from tests.testcase import BaseTestCase
from src import errors
from src.errors import ServerException


class ErrorsInsertTestCase(BaseTestCase):
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
