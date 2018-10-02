# coding=utf-8
from __future__ import unicode_literals

from tests.testcase import BaseTestCase
from clickhouse_driver import errors


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

    def test_non_utf(self):
        columns = 'a String'

        data = [('яндекс'.encode('koi8-r'), )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query, encoding='koi8-r')
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

    def test_buffer_reader(self):
        columns = 'a String'

        data = [('a' * 300, )] * 300
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)


class ByteStringTestCase(BaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True}}

    def test_not_decoded(self):
        columns = 'a String'

        data = [
            (bytearray('яндекс'.encode('cp1251')), ),
            (bytes('test'.encode('cp1251')), ),
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query, encoding='cp1251')
            self.assertEqual(inserted, 'яндекс\ntest\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
            self.assertIsInstance(inserted[0][0], bytes)
            self.assertIsInstance(inserted[1][0], bytes)

    def test_not_decoded_bytearray_expected(self):
        columns = 'a String'

        data = [('asd', )]
        with self.create_table(columns):
            with self.assertRaises(errors.TypeMismatchError) as e:
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data,
                    types_check=True
                )

                self.assertIn('Column a', str(e.exception))
