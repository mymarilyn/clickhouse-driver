from cpython cimport Py_INCREF, PyBytes_FromStringAndSize
from cpython.bytearray cimport PyByteArray_AsString
# Using python's versions of pure c memory management functions for
# proper memory statistics count.
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from libc.string cimport memcpy


cdef char * maybe_resize_c_string(char *c_string, Py_ssize_t old_size,
                                  Py_ssize_t new_size):
    if new_size > old_size:
        c_string = <char *> PyMem_Realloc(c_string, new_size)
        if not c_string:
            raise MemoryError()
    return c_string


class BufferedReader(object):
    def __init__(self, bufsize):
        self.buffer = bytearray(bufsize)

        self.position = 0
        self.current_buffer_size = 0

        super(BufferedReader, self).__init__()

    def read_into_buffer(self):
        raise NotImplementedError

    def read(self, Py_ssize_t unread):
        # When the buffer is large enough bytes read are almost
        # always hit the buffer.
        next_position = unread + self.position
        if next_position < self.current_buffer_size:
            t = self.position
            self.position = next_position
            return self.buffer[t:self.position]

        cdef char* buffer_ptr = PyByteArray_AsString(self.buffer)
        cdef Py_ssize_t read_bytes
        cdef Py_ssize_t position = self.position
        cdef Py_ssize_t current_buffer_size = self.current_buffer_size
        rv = bytes()

        while unread > 0:
            if position == current_buffer_size:
                self.read_into_buffer()
                buffer_ptr = PyByteArray_AsString(self.buffer)
                current_buffer_size = self.current_buffer_size
                position = 0

            read_bytes = min(unread, current_buffer_size - position)
            rv += PyBytes_FromStringAndSize(&buffer_ptr[position], read_bytes)
            position += read_bytes
            unread -= read_bytes

        # Restore self-variables.
        self.position = position
        self.current_buffer_size = current_buffer_size

        return bytearray(rv)

    def read_one(self):
        if self.position == self.current_buffer_size:
            self.read_into_buffer()
            self.position = 0

        rv = self.buffer[self.position]
        self.position += 1
        return rv

    def read_strings(self, Py_ssize_t n_items, int decode=0):
        """
        Python has great overhead between function calls.
        We inline strings reading logic here to avoid this overhead.
        """
        items = PyTuple_New(n_items)

        # Reduce getattr(self, ...), calls.
        buffer = self.buffer
        cdef Py_ssize_t i
        # Buffer vars
        cdef char* buffer_ptr = PyByteArray_AsString(buffer)
        cdef Py_ssize_t right, position = self.position
        cdef Py_ssize_t current_buffer_size = self.current_buffer_size
        # String length vars
        cdef Py_ssize_t size, shift
        cdef unsigned char b

        # String for decode vars.
        cdef char *c_string = NULL
        cdef Py_ssize_t c_string_size = 0, large_str_bytes

        for i in range(n_items):
            shift = size = 0

            # Read string size
            while True:
                if position == current_buffer_size:
                    self.read_into_buffer()
                    # `read_into_buffer` can override
                    # buffer, current_buffer_size
                    # We need to restore them.
                    buffer = self.buffer
                    current_buffer_size = self.current_buffer_size
                    buffer_ptr = PyByteArray_AsString(buffer)
                    position = 0

                b = buffer_ptr[position]
                position += 1

                size |= (b & 0x7f) << shift
                if b < 0x80:
                    break

                shift += 7

            right = position + size

            if decode:
                c_string = maybe_resize_c_string(c_string, c_string_size,
                                                 size + 1)
                c_string_size = max(c_string_size, size + 1)
                c_string[size] = 0
                bytes_read = 0

            # Decoding pure c strings in Cython is faster than in pure Python.
            # We need to copy it into buffer for adding null symbol at the end.
            # In ClickHouse block there is no null
            if right > current_buffer_size:
                if decode:
                    memcpy(&c_string[bytes_read], &buffer_ptr[position],
                           current_buffer_size - position)
                else:
                    rv = PyBytes_FromStringAndSize(
                        &buffer_ptr[position], current_buffer_size - position
                    )

                bytes_read = current_buffer_size - position
                # Read the rest of the string.
                while bytes_read != size:
                    position = size - bytes_read

                    self.read_into_buffer()
                    # `read_into_buffer` can override
                    # buffer, current_buffer_size
                    # We need to restore them.
                    buffer = self.buffer
                    current_buffer_size = self.current_buffer_size
                    buffer_ptr = PyByteArray_AsString(buffer)
                    # There can be not enough data in buffer.
                    position = min(position, current_buffer_size)
                    if decode:
                        memcpy(&c_string[bytes_read], buffer_ptr, position)
                    else:
                        rv += PyBytes_FromStringAndSize(buffer_ptr, position)
                    bytes_read += position

            else:
                if decode:
                    memcpy(c_string, &buffer_ptr[position], size)
                else:
                    rv = PyBytes_FromStringAndSize(&buffer_ptr[position], size)
                position = right

            if decode:
                try:
                    rv = c_string[:size].decode('utf-8')
                except UnicodeDecodeError:
                    rv = PyBytes_FromStringAndSize(c_string, size)

            Py_INCREF(rv)
            PyTuple_SET_ITEM(items, i, rv)

        if c_string:
            PyMem_Free(c_string)

        # Restore self-variables.
        self.buffer = buffer
        self.position = position
        self.current_buffer_size = current_buffer_size

        return items


class BufferedSocketReader(BufferedReader):
    def __init__(self, sock, bufsize):
        self.sock = sock
        super(BufferedSocketReader, self).__init__(bufsize)

    def read_into_buffer(self):
        self.current_buffer_size = self.sock.recv_into(self.buffer)

        if self.current_buffer_size == 0:
            raise EOFError('Unexpected EOF while reading bytes')


class CompressedBufferedReader(BufferedReader):
    def __init__(self, read_block, bufsize):
        self.read_block = read_block
        super(CompressedBufferedReader, self).__init__(bufsize)

    def read_into_buffer(self):
        self.buffer = bytearray(self.read_block())
        self.current_buffer_size = len(self.buffer)

        if self.current_buffer_size == 0:
            raise EOFError('Unexpected EOF while reading bytes')
