import struct


size_by_type = {
    'Date': 2,
    'DateTime': 4,
    'Int8': 1,
    'UInt8': 1,
    'Int16': 2,
    'UInt16': 2,
    'Int32': 4,
    'UInt32': 4,
    'Int64': 8,
    'UInt64': 8,
    'Float32': 4,
    'Float64': 8,
    'Enum8': 1,
    'Enum16': 2
}


class Column(object):
    ch_type = None
    py_types = None

    @property
    def size(self):
        return size_by_type[self.ch_type]

    def read(self, buf):
        raise NotImplementedError

    def write(self, value, buf):
        raise NotImplementedError

    def _read(self, buf, fmt):
        return struct.unpack(fmt, buf.read(self.size))[0]

    def _write(self, x, buf, fmt):
        return buf.write(struct.pack(fmt, x))

    def write_data(self, data, buf):
        for x in data:
            if not isinstance(x, self.py_types):
                raise TypeError(x)

            self.write(x, buf)

    def read_data(self, rows, buf):
        return tuple(self.read(buf) for _i in range(rows))
