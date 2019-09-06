from cpython cimport PyMem_Malloc, PyMem_Free, PyBytes_AsString, PyBytes_Check
from cpython.bytearray cimport PyByteArray_AsString, \
    PyByteArray_FromStringAndSize
from libc.string cimport memcpy

from codecs import utf_8_encode

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

        if PyBytes_Check(data):
            c_data = PyBytes_AsString(data)
        else:
            c_data = PyByteArray_AsString(data)

        while written < data_len:
            size = min(data_len - written, self.buffer_size - self.position)
            memcpy(&self.buffer[self.position], &c_data[written], size)

            if self.position == self.buffer_size:
                self.write_into_stream()

            self.position += size
            written += size

    def flush(self):
        self.write_into_stream()

    def write_strings(self, items, int encode=0):
        for value in items:
            if encode:
                if not isinstance(value, bytes):
                    value = utf_8_encode(value)[0]

            write_varint(len(value), self)
            self.write(value)


cdef class BufferedSocketWriter(BufferedWriter):
    cdef object sock

    def __init__(self, sock, bufsize):
        self.sock = sock
        super(BufferedSocketWriter, self).__init__(bufsize)

    cpdef write_into_stream(self):
        self.sock.sendall(
            PyByteArray_FromStringAndSize(self.buffer, self.position)
        )
        self.position = 0


# TODO: make proper CompressedBufferedWriter
