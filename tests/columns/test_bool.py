from tests.testcase import BaseTestCase
from clickhouse_driver import errors


class BoolTestCase(BaseTestCase):
    required_server_version = (21, 12)

    def test_simple(self):
        columns = ("a Bool")

        data = [(1,), (0,), (True,), (False,), (None,), ("False",), ("",)]
        with self.create_table(columns):
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    'true\n'
                    'false\n'
                    'true\n'
                    'false\n'
                    'false\n'
                    'true\n'
                    'false\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (True, ),
                    (False, ),
                    (True, ),
                    (False, ),
                    (False, ),
                    (True, ),
                    (False, ),
                ]
            )

    def test_errors(self):
        columns = "a Bool"
        with self.create_table(columns):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', [(1, )],
                    types_check=True
                )

    def test_nullable(self):
        columns = "a Nullable(Bool)"

        data = [(None, ), (True, ), (False, )]
        with self.create_table(columns):
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    '\\N\ntrue\nfalse\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    (None, ), (True, ), (False, ),
                ]
            )
