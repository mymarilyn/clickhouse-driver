from io import BytesIO


class BaseCompressor(object):
    """
    Partial file-like object with write method.
    """
    method = None
    method_byte = None

    def __init__(self):
        self.data = BytesIO()

        super(BaseCompressor, self).__init__()

    def write(self, p_str):
        self.data.write(p_str)

    def get_compressed_data(self, extra_header_size):
        raise NotImplementedError


class BaseDecompressor(object):
    method = None
    method_byte = None

    def __init__(self, real_stream):
        self.stream = real_stream
        super(BaseDecompressor, self).__init__()

    def get_decompressed_data(self, method_byte, compressed_hash,
                              extra_header_size):
        raise NotImplementedError
