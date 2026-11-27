from datetime import date, datetime
from io import BytesIO
from types import SimpleNamespace
from unittest import TestCase

from clickhouse_driver import errors
from clickhouse_driver.block import RowOrientedBlock
from clickhouse_driver.client import Client
from clickhouse_driver.compression import get_compressor_cls
from clickhouse_driver.compression.base import BaseCompressor
from clickhouse_driver.compression.lz4 import Compressor
from clickhouse_driver.defines import BUFFER_SIZE
from clickhouse_driver.streams.compressed import (
    CompressedBlockInputStream, CompressedBlockOutputStream
)
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


class RecordingCompressor(BaseCompressor):
    method_byte = None

    def __init__(self):
        super(RecordingCompressor, self).__init__()
        self.input_sizes = []

    def compress_data(self, data):
        self.input_sizes.append(len(data))
        return data


class CompressedOutputStreamTestCase(TestCase):
    def test_compression_input_is_bounded(self):
        payload = b'x' * (2 * BUFFER_SIZE + 123)
        stream = CompressedBlockOutputStream(
            RecordingCompressor, BUFFER_SIZE, BytesIO(), None
        )

        stream.fout.write(payload)
        stream.finalize()

        self.assertEqual(
            stream.compressor.input_sizes, [BUFFER_SIZE, BUFFER_SIZE, 123]
        )

    def test_compressed_chunks_roundtrip(self):
        payload = b'x' * (2 * BUFFER_SIZE + 123)
        output = BytesIO()
        stream = CompressedBlockOutputStream(
            Compressor, BUFFER_SIZE, output, None
        )
        stream.fout.write(payload)
        stream.finalize()

        compressed = BytesIO(output.getvalue())
        reader = CompressedBlockInputStream(compressed, None)
        chunks = []
        while compressed.tell() < len(output.getvalue()):
            chunks.append(reader.read_block())

        self.assertEqual(
            [len(chunk) for chunk in chunks], [BUFFER_SIZE, BUFFER_SIZE, 123]
        )
        self.assertEqual(b''.join(chunks), payload)

    def test_native_block_roundtrip(self):
        context = SimpleNamespace(
            server_info=SimpleNamespace(used_revision=0),
            client_settings={
                'use_numpy': False,
                'strings_as_bytes': False,
                'strings_encoding': 'utf-8',
            },
        )
        rows = [('x' * (2 * BUFFER_SIZE + 123),), ('tail',)]
        output = BytesIO()
        writer = CompressedBlockOutputStream(
            Compressor, BUFFER_SIZE, output, context
        )
        writer.write(RowOrientedBlock([('value', 'String')], rows))

        reader = CompressedBlockInputStream(
            BytesIO(output.getvalue()), context
        )
        block = reader.read()
        self.assertEqual(block.get_rows(), rows)

    def test_no_empty_chunk_after_full_buffer(self):
        output = BytesIO()
        stream = CompressedBlockOutputStream(
            Compressor, BUFFER_SIZE, output, None
        )
        stream.fout.write(b'x' * BUFFER_SIZE)
        stream.finalize()

        compressed = BytesIO(output.getvalue())
        reader = CompressedBlockInputStream(compressed, None)
        self.assertEqual(reader.read_block(), b'x' * BUFFER_SIZE)
        self.assertEqual(compressed.tell(), len(output.getvalue()))


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
