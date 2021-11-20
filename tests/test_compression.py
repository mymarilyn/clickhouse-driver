from datetime import date, datetime
from unittest import TestCase

from clickhouse_driver import errors
from clickhouse_driver.client import Client
from clickhouse_driver.compression import get_compressor_cls
from clickhouse_driver.compression.lz4 import Compressor
from .testcase import BaseTestCase, file_config


class BaseCompressionTestCase(BaseTestCase):
    compression = False
    supported_compressions = file_config.get('db', 'compression').split(',')

    def _create_client(self):
        settings = None
        if self.compression:
            # Set server compression method explicitly
            # By default server sends blocks compressed by LZ4.
            method = self.compression
            if self.server_version > (19, ):
                method = method.upper()
            settings = {'network_compression_method': method}

        return Client(
            self.host, self.port, self.database, self.user, self.password,
            compression=self.compression, settings=settings
        )

    def setUp(self):
        super(BaseCompressionTestCase, self).setUp()
        supported = (
            self.compression is False or
            self.compression in self.supported_compressions
        )

        if not supported:
            self.skipTest(
                'Compression {} is not supported'.format(self.compression)
            )

    def run_simple(self):
        with self.create_table('a Date, b DateTime'):
            data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25\t2012-10-25 14:07:19\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test(self):
        if self.compression is False:
            return

        self.run_simple()


class LZ4ReadWriteTestCase(BaseCompressionTestCase):
    compression = 'lz4'


class LZ4HCReadWriteTestCase(BaseCompressionTestCase):
    compression = 'lz4hc'


class ZSTDReadWriteTestCase(BaseCompressionTestCase):
    compression = 'zstd'


class MiscCompressionTestCase(TestCase):
    def test_default_compression(self):
        client = Client('localhost', compression=True)
        self.assertEqual(client.connection.compressor_cls, Compressor)

    def test_unknown_compressor(self):
        with self.assertRaises(errors.UnknownCompressionMethod) as e:
            get_compressor_cls('hello')

        self.assertEqual(
            e.exception.code, errors.ErrorCodes.UNKNOWN_COMPRESSION_METHOD
        )


class ReadByBlocksTestCase(BaseCompressionTestCase):
    compression = 'lz4'

    def test(self):
        with self.create_table('a Int32'):
            data = [(x % 200, ) for x in range(1000000)]

            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
