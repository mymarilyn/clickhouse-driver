from cpython cimport Py_INCREF, PyBytes_AsString, PyBytes_FromStringAndSize, \
    PyBytes_Check
from cpython.bytearray cimport PyByteArray_AsString, \
    PyByteArray_FromStringAndSize
# Using python's versions of pure c memory management functions for
# proper memory statistics count.
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython.list cimport PyList_New, PyList_SET_ITEM
from libc.string cimport memcpy, memset

from .. import errors
from ..util import compat
from .base import Column

from codecs import utf_8_encode


class String(Column):
    ch_type = 'String'
    py_types = compat.string_types
    null_value = ''

    # TODO: pass user encoding here

    def prepare_null(self, value):
        if self.nullable and value is None:
            return self.null_value, True

        else:
            return value, False

    def write_items(self, items, buf):
        buf.write_strings(items, encode=True)

    def read_items(self, n_items, buf):
        return buf.read_strings(n_items, decode=True)


class ByteString(String):
    py_types = (bytearray, bytes)
    null_value = b''

    def write_items(self, items, buf):
        buf.write_strings(items)

    def read_items(self, n_items, buf):
        return buf.read_strings(n_items)


class FixedString(String):
    ch_type = 'FixedString'

    def __init__(self, length, **kwargs):
        self.length = length
        super(FixedString, self).__init__(**kwargs)

    def read_items(self, Py_ssize_t n_items, buf):
        cdef Py_ssize_t i, j, length = self.length
        data = buf.read(length * n_items)
        cdef char* data_ptr = PyByteArray_AsString(data)

        cdef char* c_string = <char *>PyMem_Malloc(length + 1)
        if not c_string:
            raise MemoryError()
        c_string[length] = 0

        items = PyList_New(n_items)
        for i in range(n_items):
            memcpy(c_string, &data_ptr[i * length], length)

            # Get last non zero byte of string from the end.
            j = length - 1
            while j >= 0 and not c_string[j]:
                j -= 1

            try:
                item = c_string[:j + 1].decode('utf-8')
            except UnicodeDecodeError:
                item = PyBytes_FromStringAndSize(c_string, length)
            Py_INCREF(item)
            PyList_SET_ITEM(items, i, item)

        PyMem_Free(c_string)

        return items

    def write_items(self, items, buf):
        cdef Py_ssize_t buf_pos = 0
        cdef Py_ssize_t length = self.length
        cdef Py_ssize_t items_buf_size = length * len(items)

        cdef char* c_value
        cdef char* items_buf = <char *>PyMem_Malloc(items_buf_size)
        if not items_buf:
            raise MemoryError()

        memset(items_buf, 0, items_buf_size)

        for value in items:
            if not isinstance(value, bytes):
                value = utf_8_encode(value)[0]

            value_len = len(value)
            if length < value_len:
                raise errors.TooLargeStringSize()

            if PyBytes_Check(value):
                c_value = PyBytes_AsString(value)
            else:
                c_value = PyByteArray_AsString(value)

            memcpy(&items_buf[buf_pos], c_value, value_len)
            buf_pos += length

        buf.write(PyBytes_FromStringAndSize(items_buf, items_buf_size))

        PyMem_Free(items_buf)


class ByteFixedString(FixedString):
    py_types = (bytearray, bytes)
    null_value = b''

    def read_items(self, Py_ssize_t n_items, buf):
        cdef Py_ssize_t i
        cdef Py_ssize_t length = self.length
        data = buf.read(length * n_items)
        cdef char* data_ptr = PyByteArray_AsString(data)

        items = PyList_New(n_items)
        for i in range(n_items):
            item = PyBytes_FromStringAndSize(&data_ptr[i * length], length)
            Py_INCREF(item)
            PyList_SET_ITEM(items, i, item)

        return items

    def write_items(self, items, buf):
        cdef Py_ssize_t buf_pos = 0
        cdef Py_ssize_t length = self.length
        cdef Py_ssize_t items_buf_size = length * len(items)

        cdef char* c_value
        cdef char* items_buf = <char *>PyMem_Malloc(items_buf_size)
        if not items_buf:
            raise MemoryError()

        memset(items_buf, 0, items_buf_size)

        for value in items:
            value_len = len(value)
            if length < value_len:
                raise errors.TooLargeStringSize()

            if PyBytes_Check(value):
                c_value = PyBytes_AsString(value)
            else:
                c_value = PyByteArray_AsString(value)

            memcpy(&items_buf[buf_pos], c_value, value_len)
            buf_pos += length

        buf.write(PyBytes_FromStringAndSize(items_buf, items_buf_size))

        PyMem_Free(items_buf)


def create_string_column(spec, column_options):
    client_settings = column_options['context'].client_settings
    strings_as_bytes = client_settings['strings_as_bytes']

    if spec == 'String':
        cls = ByteString if strings_as_bytes else String
        return cls(**column_options)
    else:
        length = int(spec[12:-1])
        cls = ByteFixedString if strings_as_bytes else FixedString
        return cls(length, **column_options)
