"""
Reader for ClickHouse's Dynamic / Variant column families.

These are not exposed as user-facing types in clickhouse-driver yet — they
exist here so the JSON column reader can compose them instead of
hand-rolling Variant deserialization. The byte layout mirrors
``SerializationDynamic`` and ``SerializationVariant`` in ClickHouse 25.5.
"""

import io
from struct import Struct

from .base import Column
from ..reader import (
    read_binary_str,
    read_binary_uint64,
)
from ..varint import read_varint


# DynamicSerializationVersion enum (ClickHouse 25.5).
DYNAMIC_V1 = 1
DYNAMIC_V2 = 2

# VariantDiscriminatorsSerializationMode.
VARIANT_MODE_BASIC = 0
VARIANT_MODE_COMPACT = 1

# ColumnVariant::NULL_DISCRIMINATOR — the global discriminator value used
# to mark a NULL row.
NULL_DISCRIMINATOR = 0xFF

# SHARED_VARIANT_TYPE_NAME from ClickHouse src/Columns/ColumnDynamic.h.
# Participates in the alphabetical sort of variant names alongside
# user-declared variants.
SHARED_VARIANT_NAME = "SharedVariant"


class DynamicColumn(Column):
    """
    Reader for a ClickHouse ``Dynamic`` column.

    Construction does not know the inner variant types yet — those come
    from the wire in ``read_state_prefix``. The variant list always ends
    with an implicit ``SharedVariant`` (a ``ByteString`` carrying
    ``encodeDataType + serializeBinary`` blobs) that catches values
    whose type does not match any of the declared variants.
    """

    py_types = (object,)

    def __init__(self, column_by_spec_getter, shared_column_cache=None,
                 **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.shared_column_cache = shared_column_cache
        self._column_kwargs = kwargs
        self.variant_specs = []
        self.variant_columns = []
        self.discriminators_mode = VARIANT_MODE_BASIC
        super(DynamicColumn, self).__init__(**kwargs)

    def read_state_prefix(self, buf):
        # ObjectStructure stream:
        #   UInt64 LE  structure_version  (V1 = 1, V2 = 2)
        #   if V1: VarUInt num_dynamic_types (legacy max_dynamic_types
        #                                     slot, value reused for the
        #                                     variant count — discarded
        #                                     by the server too)
        #   VarUInt    num_dynamic_types (excludes the implicit
        #                                 SharedVariant)
        #   N × String variant type spec
        # SerializationVariant prefix:
        #   UInt64 LE  discriminators_mode (BASIC = 0, COMPACT = 1)
        structure_version = read_binary_uint64(buf)
        if structure_version not in (DYNAMIC_V1, DYNAMIC_V2):
            raise NotImplementedError(
                "Unsupported Dynamic serialization version {}".format(
                    structure_version))
        if structure_version == DYNAMIC_V1:
            read_varint(buf)  # legacy slot
        num_dynamic_types = read_varint(buf)
        declared_specs = [
            read_binary_str(buf) for _ in range(num_dynamic_types)
        ]

        # SerializationVariant / DataTypeVariant sort their global
        # discriminators by ``DataType::getName()`` alphabetically. The
        # SharedVariant type name ("SharedVariant") participates in the
        # sort, so we have to interleave it back in to know which
        # discriminator index it claims.
        all_names = list(declared_specs) + [SHARED_VARIANT_NAME]
        sorted_names = sorted(all_names)
        self.variant_specs = list(declared_specs)
        self.variant_columns = []
        self._shared_variant_index = None
        for i, name in enumerate(sorted_names):
            if name == SHARED_VARIANT_NAME:
                self.variant_columns.append(
                    _make_byte_string(self._column_kwargs))
                self._shared_variant_index = i
            else:
                self.variant_columns.append(
                    self.column_by_spec_getter(name))

        self.discriminators_mode = read_binary_uint64(buf)
        if self.discriminators_mode not in (
                VARIANT_MODE_BASIC, VARIANT_MODE_COMPACT):
            raise NotImplementedError(
                "Unsupported Variant discriminators mode {}".format(
                    self.discriminators_mode))
        # Variant element state prefixes — most primitives are no-ops but
        # any composed type (LowCardinality, etc.) needs to consume its
        # own bytes. The SharedVariant String has no prefix.
        for i, column in enumerate(self.variant_columns):
            if i == self._shared_variant_index:
                continue
            column.read_state_prefix(buf)

    def read_items(self, n_items, buf):
        if self.discriminators_mode == VARIANT_MODE_COMPACT:
            raise NotImplementedError(
                "Compact Variant discriminators are not supported yet")
        return _read_variant_basic(
            n_items, self.variant_columns, buf,
            shared_variant_index=self._shared_variant_index,
            shared_variant_decoder=lambda blob: decode_shared_value(
                blob, self.column_by_spec_getter,
                self.shared_column_cache))


def _make_byte_string(kwargs):
    # Avoid a circular import at module top-level.
    from .stringcolumn import ByteString
    return ByteString(**kwargs)


def _read_variant_basic(n_items, variant_columns, buf,
                        shared_variant_index=None,
                        shared_variant_decoder=None):
    """
    Read ``n_items`` rows of a SerializationVariant body in BASIC mode.

    On the wire (per ClickHouse 25.5):
      - n_items × UInt8 global discriminators (``255`` for NULL)
      - For each variant in global discriminator order, the per-variant
        column's data, sized by how many rows landed in that variant.
    """
    discriminators = buf.read(n_items)
    if len(discriminators) != n_items:
        raise EOFError(
            "Variant discriminators truncated: got {} bytes, want {}".format(
                len(discriminators), n_items))

    # Count rows per variant (preserving wire-order row appearance).
    per_variant_indices = [[] for _ in range(len(variant_columns))]
    for row, disc in enumerate(discriminators):
        if disc == NULL_DISCRIMINATOR:
            continue
        if disc >= len(variant_columns):
            raise ValueError(
                "Variant discriminator {} out of range (have {} variants)"
                .format(disc, len(variant_columns)))
        per_variant_indices[disc].append(row)

    values_by_row = [None] * n_items
    for variant, rows in enumerate(per_variant_indices):
        if not rows:
            continue
        column = variant_columns[variant]
        # ``read_data`` handles nulls maps / nested column wiring;
        # ``read_items`` is a thinner low-level API. We pick whichever
        # the underlying column exposes — most primitives implement both.
        if hasattr(column, 'read_data'):
            chunk = column.read_data(len(rows), buf)
        else:
            chunk = column.read_items(len(rows), buf)
        chunk = list(chunk)
        if variant == shared_variant_index and shared_variant_decoder:
            chunk = [shared_variant_decoder(b) for b in chunk]
        for row, value in zip(rows, chunk):
            values_by_row[row] = value

    return values_by_row


# ----------------------------------------------------------------------
# encodeDataType / serializeBinary decoder for SharedVariant blobs
# ----------------------------------------------------------------------

# BinaryTypeIndex tags from ClickHouse 25.5
# (src/DataTypes/DataTypesBinaryEncoding.cpp). Only the tags reachable
# from values that fit in a JSON document are mapped; unmapped tags
# raise on read so we surface gaps loudly instead of silently dropping
# data.
_TAG_NOTHING = 0x00
_TAG_UINT8 = 0x01
_TAG_UINT16 = 0x02
_TAG_UINT32 = 0x03
_TAG_UINT64 = 0x04
_TAG_UINT128 = 0x05
_TAG_UINT256 = 0x06
_TAG_INT8 = 0x07
_TAG_INT16 = 0x08
_TAG_INT32 = 0x09
_TAG_INT64 = 0x0A
_TAG_INT128 = 0x0B
_TAG_INT256 = 0x0C
_TAG_FLOAT32 = 0x0D
_TAG_FLOAT64 = 0x0E
_TAG_DATE = 0x0F
_TAG_DATE32 = 0x10
_TAG_DATETIME_UTC = 0x11
_TAG_DATETIME_TZ = 0x12
_TAG_DATETIME64_UTC = 0x13
_TAG_DATETIME64_TZ = 0x14
_TAG_STRING = 0x15
_TAG_FIXED_STRING = 0x16
_TAG_ARRAY = 0x1E
_TAG_TUPLE_UNNAMED = 0x1F
_TAG_TUPLE_NAMED = 0x20
_TAG_NULLABLE = 0x23
_TAG_BOOL = 0x2D


def _read_varint_bytesio(buf):
    """LEB128 reader for ``io.BytesIO`` (the Cython ``read_varint`` wants
    a ``read_one`` method which ``BytesIO`` lacks)."""
    shift = 0
    result = 0
    while True:
        byte = buf.read(1)
        if not byte:
            return result
        b = byte[0]
        result |= (b & 0x7F) << shift
        shift += 7
        if b < 0x80:
            return result


def _read_string_bytesio(buf):
    length = _read_varint_bytesio(buf)
    return buf.read(length)


def _decode_type_spec(buf):
    """
    Parse the ``encodeDataType`` byte stream into a ClickHouse type
    string suitable for ``column_by_spec_getter``.
    """
    tag = buf.read(1)
    if not tag:
        raise EOFError("Unexpected end of encoded type stream")
    tag = tag[0]

    name = _PRIMITIVE_TYPE_NAMES.get(tag)
    if name is not None:
        return name

    if tag == _TAG_FIXED_STRING:
        n = _read_varint_bytesio(buf)
        return "FixedString({})".format(n)

    if tag == _TAG_DATETIME_TZ:
        tz = _read_string_bytesio(buf).decode('utf-8')
        return "DateTime('{}')".format(tz)

    if tag == _TAG_DATETIME64_UTC:
        scale = _read_varint_bytesio(buf)
        return "DateTime64({})".format(scale)

    if tag == _TAG_DATETIME64_TZ:
        scale = _read_varint_bytesio(buf)
        tz = _read_string_bytesio(buf).decode('utf-8')
        return "DateTime64({}, '{}')".format(scale, tz)

    if tag == _TAG_NULLABLE:
        inner = _decode_type_spec(buf)
        return "Nullable({})".format(inner)

    if tag == _TAG_ARRAY:
        inner = _decode_type_spec(buf)
        return "Array({})".format(inner)

    if tag == _TAG_TUPLE_UNNAMED:
        size = _read_varint_bytesio(buf)
        elems = [_decode_type_spec(buf) for _ in range(size)]
        return "Tuple({})".format(", ".join(elems))

    if tag == _TAG_TUPLE_NAMED:
        size = _read_varint_bytesio(buf)
        parts = []
        for _ in range(size):
            element_name = _read_string_bytesio(buf).decode('utf-8')
            element_type = _decode_type_spec(buf)
            parts.append("{} {}".format(element_name, element_type))
        return "Tuple({})".format(", ".join(parts))

    raise NotImplementedError(
        "Cannot decode binary type tag 0x{:02x} in shared JSON value"
        .format(tag))


_PRIMITIVE_TYPE_NAMES = {
    _TAG_NOTHING: "Nothing",
    _TAG_UINT8: "UInt8",
    _TAG_UINT16: "UInt16",
    _TAG_UINT32: "UInt32",
    _TAG_UINT64: "UInt64",
    _TAG_UINT128: "UInt128",
    _TAG_UINT256: "UInt256",
    _TAG_INT8: "Int8",
    _TAG_INT16: "Int16",
    _TAG_INT32: "Int32",
    _TAG_INT64: "Int64",
    _TAG_INT128: "Int128",
    _TAG_INT256: "Int256",
    _TAG_FLOAT32: "Float32",
    _TAG_FLOAT64: "Float64",
    _TAG_DATE: "Date",
    _TAG_DATE32: "Date32",
    _TAG_DATETIME_UTC: "DateTime",
    _TAG_STRING: "String",
    _TAG_BOOL: "Bool",
}


def decode_shared_value(blob, column_by_spec_getter, column_cache=None):
    """
    Decode one ``encodeDataType + serializeBinary`` payload from a
    SharedVariant.

    ``serializeBinary`` is the single-field binary format, not the
    column-bulk one — so we walk the encoded type and read the value
    out by spec. Primitive scalar types delegate to the driver's
    existing readers (their ``read_items(1, buf)`` happens to match
    ``serializeBinary`` byte-for-byte). Array / Tuple / Nullable have to
    be unwrapped manually because the column readers expect offsets
    and nulls-maps that ``serializeBinary`` does not write.

    ``column_cache`` is an optional mutable dict mapping ``type_spec`` →
    column reader. Shared data is dominated by reconstructing the same
    handful of scalar column readers for every overflow value, so
    callers should pass a per-block dict to memoise them. The cache
    only covers stateless scalar readers — composite types
    (Array/Tuple/Nullable) are unwrapped before any reader is built.
    """
    if not blob:
        return None
    buf = io.BytesIO(blob)
    type_spec = _decode_type_spec(buf)
    return _decode_value_by_spec(
        type_spec, buf, column_by_spec_getter, column_cache)


def _decode_value_by_spec(type_spec, buf, column_by_spec_getter,
                          column_cache):
    if type_spec == "Nothing":
        return None

    if type_spec.startswith("Nullable("):
        is_null = buf.read(1)
        if is_null and is_null[0]:
            return None
        inner = type_spec[len("Nullable("):-1]
        return _decode_value_by_spec(
            inner, buf, column_by_spec_getter, column_cache)

    if type_spec.startswith("Array("):
        inner = type_spec[len("Array("):-1]
        size = _read_varint_bytesio(buf)
        return [
            _decode_value_by_spec(
                inner, buf, column_by_spec_getter, column_cache)
            for _ in range(size)
        ]

    if type_spec.startswith("Tuple("):
        elements = _split_tuple_elements(
            type_spec[len("Tuple("):-1])
        return tuple(
            _decode_value_by_spec(
                elem, buf, column_by_spec_getter, column_cache)
            for elem in elements
        )

    # Scalar — let the driver's column reader handle it.
    if column_cache is not None:
        column = column_cache.get(type_spec)
        if column is None:
            column = column_by_spec_getter(type_spec)
            column_cache[type_spec] = column
    else:
        column = column_by_spec_getter(type_spec)
    value = column.read_items(1, _BytesIOReader(buf))
    return list(value)[0]


def _split_tuple_elements(inner):
    """Split a ``Tuple(...)`` body on top-level commas."""
    parts = []
    depth = 0
    start = 0
    for i, ch in enumerate(inner):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif ch == ',' and depth == 0:
            parts.append(inner[start:i].strip())
            start = i + 1
    last = inner[start:].strip()
    if last:
        parts.append(last)
    return parts


class _BytesIOReader:
    """
    Adapter that gives ``io.BytesIO`` the ``read_one`` method the Cython
    ``read_varint`` expects when called from inside column readers.
    """

    _struct_one = Struct('<B')

    __slots__ = ('_buf',)

    def __init__(self, buf):
        self._buf = buf

    def read(self, n):
        return self._buf.read(n)

    def read_one(self):
        b = self._buf.read(1)
        if not b:
            raise EOFError("Unexpected end of shared value blob")
        return b[0]

    def read_strings(self, n_items, encoding=None):
        # StringColumn delegates length-prefixed reads to read_strings on
        # the buffered reader; fall back to inlining the loop here.
        out = []
        for _ in range(n_items):
            length = _read_varint_bytesio(self._buf)
            chunk = self._buf.read(length)
            if encoding is not None:
                chunk = chunk.decode(encoding)
            out.append(chunk)
        return out
