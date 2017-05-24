import struct

from six import PY3


if PY3:
    def _byte(b):
        return bytes((b, ))
else:
    def _byte(b):
        return chr(b)


def write_binary_str(text, buf):
    text = text.encode('utf-8')
    write_varint(len(text), buf)
    buf.write(text)


def write_binary_str_fixed_len(text, buf, length):
    text = text.encode('utf-8')
    diff = length - len(text)
    if diff > 0:
        text += _byte(0) * diff
    elif diff < 0:
        raise ValueError
    buf.write(text)


def write_varint(number, buf):
    """
    Writes integer of variable length using LEB128.
    """
    while True:
        towrite = number & 0x7f
        number >>= 7
        if number:
            buf.write(_byte(towrite | 0x80))
        else:
            buf.write(_byte(towrite))
            break


def write_binary_int(number, buf, fmt):
    """
    Writes int from buffer with provided format.
    """
    fmt = '<' + fmt
    buf.write(struct.pack(fmt, number))


def write_binary_int8(number, buf):
    write_binary_int(number, buf, 'b')


def write_binary_int16(number, buf):
    write_binary_int(number, buf, 'h')


def write_binary_int32(number, buf):
    write_binary_int(number, buf, 'i')


def write_binary_int64(number, buf):
    write_binary_int(number, buf, 'q')


def write_binary_uint8(number, buf):
    write_binary_int(number, buf, 'B')


def write_binary_uint16(number, buf):
    write_binary_int(number, buf, 'H')


def write_binary_uint32(number, buf):
    write_binary_int(number, buf, 'I')


def write_binary_uint64(number, buf):
    write_binary_int(number, buf, 'Q')


MAX_UINT64 = (1 << 64) - 1


def write_binary_uint128(number, buf):
    fmt = '<QQ'
    packed = struct.pack(fmt, (number >> 64) & MAX_UINT64, number & MAX_UINT64)
    buf.write(packed)
