from tests.testcase import BaseTestCase
from src import errors

ErrorCodes = errors.ErrorCodes


class NullableTestCase(BaseTestCase):
    def entuple(self, lst):
        return tuple(
            self.entuple(x) if isinstance(x, list) else x for x in lst
        )

    def test_simple(self):
        columns = 'a Nullable(Int32)'

        data = [(3, ), (None, ), (2, )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '3\n\\N\n2\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_inside_nullable(self):
        columns = 'a Nullable(Nullable(Int32))'

        data = [(3, )]
        with self.create_table(columns):
            with self.assertRaises(errors.ServerException) as e:
                self.client.execute('INSERT INTO test (a) VALUES', data)

            self.assertEqual(e.exception.code, ErrorCodes.ILLEGAL_COLUMN)

    def test_nullable_array(self):
        columns = 'a Nullable(Array(Nullable(Array(Nullable(Int32)))))'

        data = [
            (self.entuple([[1]]), ),
            (None, ),
            (self.entuple([None]), ),
            (self.entuple([[4]]), ),
            (self.entuple([[None]]), )
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '[[1]]\n\\N\n[NULL]\n[[4]]\n[[NULL]]\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
