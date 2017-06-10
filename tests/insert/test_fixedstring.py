# coding=utf-8
from __future__ import unicode_literals

from tests.testcase import BaseTestCase
from src import errors


class FixedStringTestCase(BaseTestCase):
    def test_simple(self):
        columns = 'a FixedString(4)'

        data = [('a', ), ('bb', ), ('ccc', ), ('dddd', ), ('я', )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    'a\\0\\0\\0\n'
                    'bb\\0\\0\n'
                    'ccc\\0\n'
                    'dddd\n'
                    'я\\0\\0\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_oversized(self):
        columns = 'a FixedString(4)'

        data = [('aaaaa', )]
        with self.create_table(columns):
            with self.assertRaises(errors.TooLargeStringSize):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

        data = [('тест', )]
        with self.create_table(columns):
            with self.assertRaises(errors.TooLargeStringSize):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )
