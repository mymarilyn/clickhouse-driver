# cython: infer_types=True

"""
A minimal Python wrapper around CityHash 1.0.2, the version of the
non-cryptographic hash algorithm used by ClickHouse for block checksums.

Vendored from the clickhouse-cityhash package; see LICENSE in this directory.
"""

__all__ = ["CityHash128"]

cdef extern from "city.h" nogil:
    ctypedef unsigned long long uint64
    ctypedef struct uint128:
        uint64 first
        uint64 second
    uint128 c_CityHash128 "CityHash128" (const char *s, size_t length)


from cpython.buffer cimport PyObject_CheckBuffer
from cpython.buffer cimport PyBUF_SIMPLE
from cpython.buffer cimport Py_buffer
from cpython.buffer cimport PyObject_GetBuffer

from cpython.unicode cimport PyUnicode_Check
from cpython.unicode cimport PyUnicode_AsUTF8String

from cpython.bytes cimport PyBytes_Check
from cpython.bytes cimport PyBytes_GET_SIZE
from cpython.bytes cimport PyBytes_AS_STRING

from cpython cimport Py_DECREF


cdef object _type_error(str argname, expected, value):
    return TypeError(
        "Argument '%s' has incorrect type (expected %s, got %s)" %
        (argname, expected, type(value))
    )


cpdef CityHash128(data):
    """128-bit hash function for a bytes, str or buffer type."""
    cdef Py_buffer buf
    cdef object obj
    cdef uint128 result
    if PyUnicode_Check(data):
        obj = PyUnicode_AsUTF8String(data)
        PyObject_GetBuffer(obj, &buf, PyBUF_SIMPLE)
        result = c_CityHash128(<const char*>buf.buf, buf.len)
        Py_DECREF(obj)
    elif PyBytes_Check(data):
        result = c_CityHash128(<const char*>PyBytes_AS_STRING(data),
                               PyBytes_GET_SIZE(data))
    elif PyObject_CheckBuffer(data):
        PyObject_GetBuffer(data, &buf, PyBUF_SIMPLE)
        result = c_CityHash128(<const char*>buf.buf, buf.len)
    else:
        raise _type_error("data", ["bytes", "str", "buffer"], data)
    return 0x10000000000000000 * int(result.first) + int(result.second)
