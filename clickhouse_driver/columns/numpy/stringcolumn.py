from ... import defines
from ..base import CommonSerialization
from .base import NumpyColumn


class ArrowStringBuffers(object):
    """
    String column read into Arrow-style buffers: concatenated bytes
    plus int64 offsets, no per-string Python objects. Assembled into
    a pyarrow array by ``clickhouse_driver.arrow.convert``.
    """
    __slots__ = ('offsets', 'data', 'nulls_map')

    def __init__(self, offsets, data, nulls_map=None):
        self.offsets = offsets
        self.data = data
        self.nulls_map = nulls_map

    def __len__(self):
        return len(self.offsets) // 8 - 1


class NumpyStringColumn(NumpyColumn):
    null_value = ''

    default_encoding = defines.STRINGS_ENCODING

    # Reset for columns whose consumers need real items, e.g.
    # LowCardinality dictionaries.
    arrow_buffers_allowed = True

    def __init__(self, encoding=default_encoding, **kwargs):
        self.encoding = encoding
        super(NumpyStringColumn, self).__init__(**kwargs)

    def _use_arrow_buffers(self, buf):
        return (
            self.arrow_buffers_allowed and
            self.use_arrow and
            self.encoding.lower() in ('utf-8', 'utf8') and
            type(self.serialization) is CommonSerialization and
            hasattr(buf, 'read_strings_arrow')
        )

    def _read_data(self, n_items, buf, nulls_map=None):
        if self._use_arrow_buffers(buf):
            offsets, data = buf.read_strings_arrow(n_items)
            return ArrowStringBuffers(offsets, data, nulls_map=nulls_map)

        return super(NumpyStringColumn, self)._read_data(
            n_items, buf, nulls_map=nulls_map
        )

    def read_items(self, n_items, buf):
        return self._wrap_string_items(
            buf.read_strings(n_items, encoding=self.encoding)
        )

    def write_items(self, items, buf):
        return buf.write_strings(items.tolist(), encoding=self.encoding)


class NumpyByteStringColumn(NumpyStringColumn):
    null_value = b''

    def _use_arrow_buffers(self, buf):
        # No decoding happens for byte strings: buffers can be used
        # regardless of encoding.
        return (
            self.arrow_buffers_allowed and
            self.use_arrow and
            type(self.serialization) is CommonSerialization and
            hasattr(buf, 'read_strings_arrow')
        )

    def read_items(self, n_items, buf):
        return self._wrap_string_items(buf.read_strings(n_items))

    def write_items(self, items, buf):
        return buf.write_strings(items.tolist())


class NumpyFixedString(NumpyStringColumn):
    def __init__(self, length, **kwargs):
        self.length = length
        super(NumpyFixedString, self).__init__(**kwargs)

    def _use_arrow_buffers(self, buf):
        # Fixed strings are trimmed of zero bytes on read: buffers
        # would keep the padding.
        return False

    def read_items(self, n_items, buf):
        return self._wrap_string_items(buf.read_fixed_strings(
            n_items, self.length, encoding=self.encoding
        ))

    def write_items(self, items, buf):
        return buf.write_fixed_strings(
            items.tolist(), self.length, encoding=self.encoding
        )


class NumpyByteFixedString(NumpyByteStringColumn):
    def __init__(self, length, **kwargs):
        self.length = length
        super(NumpyByteFixedString, self).__init__(**kwargs)

    def _use_arrow_buffers(self, buf):
        return False

    def read_items(self, n_items, buf):
        return self._wrap_string_items(
            buf.read_fixed_strings(n_items, self.length)
        )

    def write_items(self, items, buf):
        return buf.write_fixed_strings(items.tolist(), self.length)


def create_string_column(spec, column_options):
    client_settings = column_options['context'].client_settings
    strings_as_bytes = client_settings['strings_as_bytes']
    encoding = client_settings.get(
        'strings_encoding', NumpyStringColumn.default_encoding
    )

    if spec == 'String':
        cls = NumpyByteStringColumn if strings_as_bytes else NumpyStringColumn
        return cls(encoding=encoding, **column_options)
    else:
        length = int(spec[12:-1])
        cls = NumpyByteFixedString if strings_as_bytes else NumpyFixedString
        return cls(length, encoding=encoding, **column_options)
