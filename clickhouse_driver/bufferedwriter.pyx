from cpython cimport PyMem_Malloc, PyMem_Free, PyBytes_AsString, \
    PyBytes_Check, PyBytes_FromStringAndSize
from libc.string cimport memcpy

from .varint import write_varint


cdef class BufferedWriter(object):
    cdef char* buffer
    cdef Py_ssize_t position, buffer_size

    def __init__(self, Py_ssize_t bufsize):
        self.buffer = <char *> PyMem_Malloc(bufsize)
        if not self.buffer:
            raise MemoryError()

        self.position = 0
        self.buffer_size = bufsize

        super(BufferedWriter, self).__init__()

    def __dealloc__(self):
        PyMem_Free(self.buffer)

    cpdef write_into_stream(self):
        raise NotImplementedError

    cpdef write(self, data):
        cdef Py_ssize_t written = 0
        cdef Py_ssize_t to_write, size
        cdef Py_ssize_t data_len = len(data)
        cdef char* c_data

        c_data = PyBytes_AsString(data)

        while written < data_len:
            size = min(data_len - written, self.buffer_size - self.position)
            memcpy(&self.buffer[self.position], &c_data[written], size)

            if self.position == self.buffer_size:
                self.write_into_stream()

            self.position += size
            written += size

    def flush(self):
        self.write_into_stream()

    def write_strings(self, items, encoding=None):
        cdef int do_encode = encoding is not None

        for value in items:
            if not PyBytes_Check(value):
                if do_encode:
                    value = value.encode(encoding)
                else:
                    raise ValueError('bytes object expected')

            write_varint(len(value), self)
            self.write(value)


cdef class BufferedSocketWriter(BufferedWriter):
    cdef object sock

    def __init__(self, sock, bufsize):
        self.sock = sock
        super(BufferedSocketWriter, self).__init__(bufsize)

    cpdef write_into_stream(self):
        self.sock.sendall(
            PyBytes_FromStringAndSize(self.buffer, self.position)
        )
        self.position = 0


cdef class CompressedBufferedWriter(BufferedWriter):
    cdef object compressor

    def __init__(self, compressor, bufsize):
        self.compressor = compressor
        super(CompressedBufferedWriter, self).__init__(bufsize)

    cpdef write_into_stream(self):
        self.compressor.write(
            PyBytes_FromStringAndSize(self.buffer, self.position)
        )
        self.position = 0

    def flush(self):
        self.write_into_stream()
