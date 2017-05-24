import struct

size_by_format = {
    'c': 1,
    'b': 1,
    'B': 1,
    '?': 1,
    'h': 2,
    'H': 2,
    'i': 4,
    'I': 4,
    'l': 4,
    'L': 4,
    'q': 8,
    'Q': 8,
    'f': 4,
    'd': 8
}


def read_binary_str(f):
    length = read_varint(f)
    return f.read(length).decode('utf-8')


def read_binary_str_fixed_len(f, length):
    return f.read(length).decode('utf-8')


def _read_one(f):
    c = f.read(1)
    if c == '':
        raise EOFError("Unexpected EOF while reading bytes")

    return ord(c)


def read_varint(f):
    """
    Reads integer of variable length using LEB128.
    """
    shift = 0
    result = 0

    while True:
        i = _read_one(f)
        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            break

    return result


def read_binary_int(buf, fmt):
    """
    Reads int from buffer with provided format.
    """
    size = size_by_format[fmt]
    # Little endian.
    fmt = '<' + fmt
    return struct.unpack(fmt, buf.read(size))[0]


def read_binary_int8(buf):
    return read_binary_int(buf, 'b')


def read_binary_int16(buf):
    return read_binary_int(buf, 'h')


def read_binary_int32(buf):
    return read_binary_int(buf, 'i')


def read_binary_int64(buf):
    return read_binary_int(buf, 'q')


def read_binary_uint8(buf):
    return read_binary_int(buf, 'B')


def read_binary_uint16(buf):
    return read_binary_int(buf, 'H')


def read_binary_uint32(buf):
    return read_binary_int(buf, 'I')


def read_binary_uint64(buf):
    return read_binary_int(buf, 'Q')


def read_binary_uint128(buf):
    hi = read_binary_int(buf, 'Q')
    lo = read_binary_int(buf, 'Q')

    return (hi << 64) + lo
