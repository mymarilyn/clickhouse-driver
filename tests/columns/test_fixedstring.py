# coding=utf-8
from __future__ import unicode_literals

from tests.testcase import BaseTestCase
from clickhouse_driver import errors


class FixedStringTestCase(BaseTestCase):
    def test_simple(self):
        data = [('a', ), ('bb', ), ('ccc', ), ('dddd', ), ('я', )]
        with self.create_table('a FixedString(4)'):
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

    def test_non_utf(self):
        data = [('яндекс'.encode('koi8-r'), )]
        with self.create_table('a FixedString(6)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query, encoding='koi8-r')
            self.assertEqual(inserted, 'яндекс\n')

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

    def test_nullable(self):
        with self.create_table('a Nullable(FixedString(10))'):
            data = [(None, ), ('test', ), (None, ), ('nullable', )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\\N\ntest\\0\\0\\0\\0\\0\\0\n\\N\nnullable\\0\\0\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_null_byte_in_the_middle(self):
        data = [('test\0test', )]
        with self.create_table('a FixedString(9)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_empty(self):
        data = [('',)]
        with self.create_table('a FixedString(5)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_custom_encoding(self):
        settings = {'strings_encoding': 'cp1251'}

        data = [(('яндекс'), ), (('test'), )]
        with self.create_table('a FixedString(10)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, settings=settings
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query, encoding='cp1251')
            self.assertEqual(
                inserted,
                'яндекс\\0\\0\\0\\0\n'
                'test\\0\\0\\0\\0\\0\\0\n'
            )

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


class ByteFixedStringTestCase(BaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True}}

    def test_oversized(self):
        columns = 'a FixedString(4)'

        data = [(bytes('aaaaa'.encode('utf-8')), )]
        with self.create_table(columns):
            with self.assertRaises(errors.TooLargeStringSize):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

        data = [(bytes('тест'.encode('utf-8')), )]
        with self.create_table(columns):
            with self.assertRaises(errors.TooLargeStringSize):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_not_decoded(self):
        data = [
            (bytes('яндекс'.encode('cp1251')), ),
            (bytes('test'.encode('cp1251')), ),
        ]
        with self.create_table('a FixedString(8)'):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query, encoding='cp1251')
            self.assertEqual(
                inserted, 'яндекс\\0\\0\ntest\\0\\0\\0\\0\n'
            )

            inserted = self.client.execute(query)
            # Assert items with trailing zeros
            self.assertEqual(
                inserted, [
                    ('яндекс'.encode('cp1251') + b'\x00' * 2, ),
                    ('test'.encode('cp1251') + b'\x00' * 4, )
                ]
            )
            self.assertIsInstance(inserted[0][0], bytes)
            self.assertIsInstance(inserted[1][0], bytes)

    def test_nullable(self):
        with self.create_table('a Nullable(FixedString(10))'):
            data = [
                (None, ),
                (b'test\x00\x00\x00\x00\x00\x00', ),
                (None, ),
                (b'nullable\x00\x00', )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\\N\ntest\\0\\0\\0\\0\\0\\0\n\\N\nnullable\\0\\0\n'
            )

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
