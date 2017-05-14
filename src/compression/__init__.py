import importlib

from ..protocol import CompressionMethodByte


def get_compressor_cls(alg):
    module = importlib.import_module('.' + alg, __name__)
    return module.Compressor


def get_decompressor_cls(method_type):
    if method_type == CompressionMethodByte.LZ4:
        module = importlib.import_module('.lz4', __name__)

    return module.Decompressor
