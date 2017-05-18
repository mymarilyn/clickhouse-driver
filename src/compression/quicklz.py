from __future__ import absolute_import
from io import BytesIO

import quicklz

from .base import BaseCompressor, BaseDecompressor
from ..protocol import CompressionMethod
from ..reader import read_binary_uint32, read_binary_uint8
from ..writer import write_binary_uint32, write_binary_uint8


class Compressor(BaseCompressor):
    method = CompressionMethod.QUICK_LZ

    def get_compressed_data(self, extra_header_size):
        compressed = BytesIO(quicklz.compress(self.data.getvalue()))

        block_size = read_binary_uint8(compressed)
        rv = BytesIO()
        write_binary_uint8(block_size & 3, rv)
        rv.write(compressed.read())
        return rv.getvalue()


class Decompressor(BaseDecompressor):
    method = CompressionMethod.QUICK_LZ

    def _get_compressed_size(self, stream):
        block_size = read_binary_uint8(stream)
        big_block = (block_size & 2) == 2

        if big_block:
            read = read_binary_uint32
            write = write_binary_uint32
            s = 4

        else:
            read = read_binary_uint8
            write = write_binary_uint32
            s = 1

        r = read(stream)
        size = r & (0xffffffff >> ((4 - s) * 8))

        header = BytesIO()
        write_binary_uint8(block_size, header)
        write(r, header)

        return size - (1 + s), header.getvalue()

    def get_decompressed_data(self, method_byte, compressed_hash,
                              extra_header_size):
        remain_size, header_bytes = self._get_compressed_size(self.stream)

        compressed = BytesIO(header_bytes)
        compressed.write(self.stream.read(remain_size))

        self.check_hash(compressed.getvalue(), compressed_hash)

        return quicklz.decompress(compressed)
