from cpython cimport Py_INCREF, PyBytes_FromStringAndSize


def write_varint(Py_ssize_t number, buf):
    """
    Writes integer of variable length using LEB128.
    """
    cdef Py_ssize_t i = 0
    cdef unsigned char towrite
    # Py_ssize_t checks integer on function call and
    # raises OverflowError if integer overflows Py_ssize_t.
    # Long enough for handling Py_ssize_t.
    cdef unsigned char num_buf[32]

    while True:
        towrite = number & 0x7f
        number >>= 7
        if number:
            num_buf[i] = towrite | 0x80
            i += 1
        else:
            num_buf[i] = towrite
            i += 1
            break

    buf.write(PyBytes_FromStringAndSize(<char *>num_buf, i))


def read_varint(f):
    """
    Reads integer of variable length using LEB128.
    """
    cdef Py_ssize_t shift = 0
    cdef Py_ssize_t result = 0
    cdef unsigned char i

    while True:
        i = f.read_one()
        result |= (i & 0x7f) << shift
        shift += 7
        if i < 0x80:
            break

    return result
