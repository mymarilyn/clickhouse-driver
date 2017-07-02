# coding=utf-8
from __future__ import unicode_literals

from tests.testcase import BaseTestCase


class StringTestCase(BaseTestCase):
    def test_unicode(self):
        columns = 'a String'

        data = [('яндекс', )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'яндекс\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        with self.create_table('a Nullable(String)'):
            data = [(None, ), ('test', ), (None, ), ('nullable', )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '\\N\ntest\n\\N\nnullable\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
