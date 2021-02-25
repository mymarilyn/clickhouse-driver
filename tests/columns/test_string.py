# coding=utf-8
from __future__ import unicode_literals

from tests.testcase import BaseTestCase
from clickhouse_driver import errors


class StringTestCase(BaseTestCase):
    def test_unicode(self):
        data = [('яндекс', )]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'яндекс\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_non_utf(self):
        data = [('яндекс'.encode('koi8-r'), )]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query, encoding='koi8-r')
            self.assertEqual(inserted, 'яндекс\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_null_byte_in_the_middle(self):
        data = [('a\x00b', )]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, 'a\\0b\n')

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
        data = [('a' * 300, )] * 300
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_compressed_client(self):
        with self.created_client(compression=True) as client:
            data = [('a' * 300, )]
            with self.create_table('a String'):
                client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

                query = 'SELECT * FROM test'

                inserted = client.execute(query)
                self.assertEqual(inserted, data)

    def test_custom_encoding(self):
        settings = {'strings_encoding': 'cp1251'}

        data = [(('яндекс'), ), (('test'), )]
        with self.create_table('a String'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, settings=settings
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query, encoding='cp1251')
            self.assertEqual(inserted, 'яндекс\ntest\n')

            inserted = self.client.execute(query, settings=settings)
            self.assertEqual(inserted, data)
            self.assertIsInstance(inserted[0][0], str)
            self.assertIsInstance(inserted[1][0], str)

    def test_not_supported_types(self):
        datas = [
            [(bytearray(b'asd'), )],
            [(123, )]
        ]
        with self.create_table('a String'):
            for data in datas:
                with self.assertRaises(errors.TypeMismatchError) as e:
                    self.client.execute(
                        'INSERT INTO test (a) VALUES', data,
                        types_check=True
                    )

                self.assertIn('for column "a"', str(e.exception))

                with self.assertRaises(AttributeError):
                    self.client.execute(
                        'INSERT INTO test (a) VALUES', data
                    )


class ByteStringTestCase(BaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True}}

    def test_not_decoded(self):
        data = [
            (bytes('яндекс'.encode('cp1251')), ),
            (bytes('test'.encode('cp1251')), ),
        ]
        with self.create_table('a String'):
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

    def test_nullable(self):
        with self.create_table('a Nullable(String)'):
            data = [(None, ), (b'test', ), (None, ), (b'nullable', )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '\\N\ntest\n\\N\nnullable\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_not_supported_types(self):
        datas = [
            [('asd', )],
            [(bytearray(b'asd'), )],
            [(123, )]
        ]
        with self.create_table('a String'):
            for data in datas:
                with self.assertRaises(errors.TypeMismatchError) as e:
                    self.client.execute(
                        'INSERT INTO test (a) VALUES', data,
                        types_check=True
                    )

                self.assertIn('for column "a"', str(e.exception))

                with self.assertRaises(ValueError) as e:
                    self.client.execute(
                        'INSERT INTO test (a) VALUES', data
                    )

                self.assertIn('bytes object expected', str(e.exception))
