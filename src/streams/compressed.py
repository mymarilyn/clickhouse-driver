from contextlib import contextmanager
from io import BytesIO

from cityhash import CityHash128

from .native import BlockOutputStream, BlockInputStream
from ..reader import read_binary_uint8, read_binary_uint128
from ..writer import write_binary_uint8, write_binary_uint128
from ..compression import get_decompressor_cls


class CompressedBlockOutputStream(BlockOutputStream):
    def __init__(self, compressor_cls, fout, server_revision):
        self.compressor_cls = compressor_cls
        self.raw_fout = fout

        self.compressor = self.compressor_cls()
        super(CompressedBlockOutputStream, self).__init__(self.compressor,
                                                          server_revision)

    def reset(self):
        self.compressor = self.compressor_cls()
        self.fout = self.compressor

    def get_compressed_hash(self, data):
        return CityHash128(data)

    def finalize(self):
        compressed = self.get_compressed()

        compressed_hash = self.get_compressed_hash(compressed)
        write_binary_uint128(compressed_hash, self.raw_fout)

        self.raw_fout.write(compressed)
        self.raw_fout.flush()

    def get_compressed(self):
        compressed = BytesIO()

        write_binary_uint8(self.compressor.method_byte, compressed)
        extra_header_size = 1  # method
        data = self.compressor.get_compressed_data(extra_header_size)
        compressed.write(data)

        return compressed.getvalue()


class CompressedBlockInputStream(BlockInputStream):
    @contextmanager
    def replace_fin(self, fin):
        old = self.fin
        self.fin = fin
        try:
            yield

        finally:
            self.fin = old

    def get_compressed_hash(self, data):
        return CityHash128(data)

    def read(self):
        compressed_hash = read_binary_uint128(self.fin)
        method_byte = read_binary_uint8(self.fin)

        decompressor_cls = get_decompressor_cls(method_byte)
        decompressor = decompressor_cls(self.fin)

        extra_header_size = 1  # method
        decompressed = decompressor.get_decompressed_data(
            method_byte, compressed_hash, extra_header_size
        )

        with self.replace_fin(BytesIO(decompressed)):
            return super(CompressedBlockInputStream, self).read()
