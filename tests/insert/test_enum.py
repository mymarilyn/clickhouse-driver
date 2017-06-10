from enum import Enum

from tests.testcase import BaseTestCase
from src import errors


class A(Enum):
    hello = -1
    world = 2


class B(Enum):
    foo = -300
    bar = 300


class EnumTestCase(BaseTestCase):
    def test_simple(self):
        columns = (
            "a Enum8('hello' = -1, 'world' = 2), "
            "b Enum16('foo' = -300, 'bar' = 300)"
        )

        data = [(A.hello, B.bar), (A.world, B.foo), (-1, 300), (2, -300)]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    'hello\tbar\n'
                    'world\tfoo\n'
                    'hello\tbar\n'
                    'world\tfoo\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted, [
                    ('hello', 'bar'), ('world', 'foo'),
                    ('hello', 'bar'), ('world', 'foo')
                ]
            )

    def test_enum_by_string(self):
        columns = "a Enum8('hello' = 1, 'world' = 2)"
        data = [('hello', ), ('world', )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    'hello\n'
                    'world\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_errors(self):
        columns = "a Enum8('test' = 1, 'me' = 2)"
        data = [(A.world, )]
        with self.create_table(columns):
            with self.assertRaises(errors.LogicalError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

        columns = "a Enum8('test' = 1, 'me' = 2)"
        data = [(3, )]
        with self.create_table(columns):
            with self.assertRaises(errors.LogicalError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_quote_in_name(self):
        columns = "a Enum8(' \\' t = ' = -1, 'test' = 2)"
        data = [(-1, ), (" \\' t = ", )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    " \\' t = \n"
                    " \\' t = \n"
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, [(" \\' t = ", ), (" \\' t = ", )])
