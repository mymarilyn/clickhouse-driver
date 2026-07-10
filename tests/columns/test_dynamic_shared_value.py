from unittest import TestCase

from clickhouse_driver.context import Context
from clickhouse_driver.columns.service import get_column_by_spec
from clickhouse_driver.columns.dynamiccolumn import (
    DynamicColumn,
    SharedValueDecoder,
    _decode_type_spec,
    _split_tuple_elements,
    _SharedValueReader,
)
from clickhouse_driver.columns.newjsoncolumn import (
    NewJsonColumn,
    OBJECT_STRING,
    _tuples_to_lists,
)


def _varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _binary_string(text):
    raw = text.encode('utf-8')
    return _varint(len(raw)) + raw


def _make_context():
    context = Context()
    context.client_settings = {
        'use_numpy': False,
        'input_format_null_as_default': False,
        'strings_as_bytes': False,
        'strings_encoding': 'utf-8',
    }
    return context


def _make_getter(context=None):
    """A column-by-spec getter matching the one the driver wires into the
    JSON reader, but standing alone so SharedVariant decoding can be
    tested without a live server."""
    context = context or _make_context()
    options = {'context': context, 'has_custom_serialization': False}

    def getter(spec):
        return get_column_by_spec(spec, options, use_numpy=False)

    return getter


class DecodeTypeSpecTestCase(TestCase):
    """``_decode_type_spec`` turns an ``encodeDataType`` byte stream into a
    ClickHouse type string. These tags are not reachable from JSON input
    on the supported server versions, so they are exercised directly."""

    def decode(self, blob):
        return _decode_type_spec(_SharedValueReader_for(blob))

    def test_primitives(self):
        self.assertEqual(self.decode(bytes([0x0A])), "Int64")
        self.assertEqual(self.decode(bytes([0x15])), "String")
        self.assertEqual(self.decode(bytes([0x2D])), "Bool")
        self.assertEqual(self.decode(bytes([0x00])), "Nothing")

    def test_fixed_string(self):
        self.assertEqual(
            self.decode(bytes([0x16]) + _varint(7)), "FixedString(7)")

    def test_datetime_with_timezone(self):
        self.assertEqual(
            self.decode(bytes([0x12]) + _binary_string("UTC")),
            "DateTime('UTC')")

    def test_datetime64_utc(self):
        self.assertEqual(
            self.decode(bytes([0x13]) + _varint(3)), "DateTime64(3)")

    def test_datetime64_with_timezone(self):
        self.assertEqual(
            self.decode(
                bytes([0x14]) + _varint(6) + _binary_string("Europe/Moscow")),
            "DateTime64(6, 'Europe/Moscow')")

    def test_nullable_and_array(self):
        self.assertEqual(
            self.decode(bytes([0x23, 0x0A])), "Nullable(Int64)")
        self.assertEqual(
            self.decode(bytes([0x1E, 0x15])), "Array(String)")

    def test_unnamed_tuple(self):
        self.assertEqual(
            self.decode(bytes([0x1F]) + _varint(2) + bytes([0x0A, 0x15])),
            "Tuple(Int64, String)")

    def test_named_tuple(self):
        blob = (
            bytes([0x20]) + _varint(2)
            + _binary_string("a") + bytes([0x0A])
            + _binary_string("b") + bytes([0x15])
        )
        self.assertEqual(self.decode(blob), "Tuple(a Int64, b String)")

    def test_json(self):
        # JSON tag (0x30) followed by its encoded parameters: version,
        # max_dynamic_paths, max_dynamic_types, and three empty path
        # lists. The value layout does not depend on them, so the spec
        # collapses to "JSON".
        blob = bytes([0x30, 0x00]) + _varint(0) + bytes([0x00]) \
            + _varint(0) + _varint(0) + _varint(0)
        self.assertEqual(self.decode(blob), "JSON")

    def test_json_with_typed_skip_and_regexp_paths(self):
        # Non-empty typed paths (name + nested type), skip paths and skip
        # regexps must all be consumed, and the spec still collapses to
        # "JSON".
        blob = (
            bytes([0x30, 0x00]) + _varint(8) + bytes([0x10])
            + _varint(1) + _binary_string("ts") + bytes([0x0A])  # ts Int64
            + _varint(1) + _binary_string("secret")              # skip path
            + _varint(1) + _binary_string("^tmp")                # skip regexp
        )
        self.assertEqual(self.decode(blob), "JSON")

    def test_unknown_tag_raises(self):
        with self.assertRaises(NotImplementedError):
            self.decode(bytes([0xFF]))

    def test_truncated_stream_raises(self):
        with self.assertRaises(EOFError):
            self.decode(b"")


class SplitTupleElementsTestCase(TestCase):
    def test_flat(self):
        self.assertEqual(
            _split_tuple_elements("Int64, String"), ["Int64", "String"])

    def test_nested_parens_are_kept_together(self):
        self.assertEqual(
            _split_tuple_elements("Array(Int64), Tuple(Int64, String)"),
            ["Array(Int64)", "Tuple(Int64, String)"])

    def test_empty(self):
        self.assertEqual(_split_tuple_elements(""), [])


class SharedValueDecoderTestCase(TestCase):
    def setUp(self):
        self.decoder = SharedValueDecoder(_make_getter())

    def test_empty_blob_is_none(self):
        self.assertIsNone(self.decoder.decode(b""))

    def test_scalar(self):
        blob = bytes([0x0A]) + (42).to_bytes(8, 'little')
        self.assertEqual(self.decoder.decode(blob), 42)

    def test_nothing(self):
        self.assertIsNone(self.decoder.decode(bytes([0x00])))

    def test_nullable_null_and_value(self):
        self.assertIsNone(self.decoder.decode(bytes([0x23, 0x0A, 0x01])))
        value = bytes([0x23, 0x0A, 0x00]) + (7).to_bytes(8, 'little')
        self.assertEqual(self.decoder.decode(value), 7)

    def test_array(self):
        blob = (
            bytes([0x1E, 0x0A]) + _varint(2)
            + (1).to_bytes(8, 'little') + (2).to_bytes(8, 'little')
        )
        self.assertEqual(self.decoder.decode(blob), [1, 2])

    def test_tuple(self):
        blob = (
            bytes([0x1F]) + _varint(2) + bytes([0x0A, 0x15])
            + (5).to_bytes(8, 'little')
            + _binary_string("x")
        )
        self.assertEqual(self.decoder.decode(blob), (5, "x"))

    def test_json_value(self):
        # A nested JSON value: header + paths. "p" is Int64(1), "a.b" is
        # String("x") (a dotted path that denormalises into a nested
        # dict), and "gone" carries a Nothing value that must be dropped.
        header = (
            bytes([0x30, 0x00]) + _varint(0) + bytes([0x00])
            + _varint(0) + _varint(0) + _varint(0)
        )
        value = (
            _varint(3)
            + _binary_string("p") + bytes([0x0A]) + (1).to_bytes(8, 'little')
            + _binary_string("a.b") + bytes([0x15]) + _binary_string("x")
            + _binary_string("gone") + bytes([0x00])
        )
        self.assertEqual(
            self.decoder.decode(header + value),
            {"p": 1, "a": {"b": "x"}})

    def test_array_of_json(self):
        # Array(JSON) — the shape that previously raised on tag 0x30.
        json_header = (
            bytes([0x30, 0x00]) + _varint(0) + bytes([0x00])
            + _varint(0) + _varint(0) + _varint(0)
        )

        def json_value(name, ivalue):
            return (
                _varint(1) + _binary_string(name) + bytes([0x0A])
                + (ivalue).to_bytes(8, 'little')
            )

        blob = (
            bytes([0x1E]) + json_header + _varint(2)
            + json_value("p", 1) + json_value("q", 2)
        )
        self.assertEqual(
            self.decoder.decode(blob), [{"p": 1}, {"q": 2}])

    def test_handler_cache_reuse(self):
        # Decoding the same spec twice must reuse the cached handler.
        first = bytes([0x0A]) + (1).to_bytes(8, 'little')
        second = bytes([0x0A]) + (2).to_bytes(8, 'little')
        self.assertEqual(self.decoder.decode(first), 1)
        self.assertEqual(self.decoder.decode(second), 2)


class DynamicColumnStatePrefixTestCase(TestCase):
    """Defensive guards in ``DynamicColumn`` that the JSON happy path never
    triggers."""

    def setUp(self):
        self.ctx = _make_context()

    def test_builds_default_shared_value_decoder(self):
        # Constructed without an explicit decoder, the column makes its own.
        column = DynamicColumn(_make_getter(self.ctx), context=self.ctx)
        self.assertIsInstance(
            column.shared_value_decoder, SharedValueDecoder)

    def test_unsupported_structure_version_raises(self):
        column = DynamicColumn(_make_getter(self.ctx), context=self.ctx)
        buf = _ListReader([(99).to_bytes(8, 'little')])
        with self.assertRaises(NotImplementedError):
            column.read_state_prefix(buf)

    def test_unsupported_discriminators_mode_raises(self):
        column = DynamicColumn(_make_getter(self.ctx), context=self.ctx)
        buf = _ListReader([
            (2).to_bytes(8, 'little'),  # structure version V2
            _varint(0),                 # zero declared variants
            (5).to_bytes(8, 'little'),  # bogus discriminators mode
        ])
        with self.assertRaises(NotImplementedError):
            column.read_state_prefix(buf)

    def test_compact_discriminators_mode_not_supported(self):
        column = DynamicColumn(_make_getter(self.ctx), context=self.ctx)
        column.discriminators_mode = 1  # VARIANT_MODE_COMPACT
        with self.assertRaises(NotImplementedError):
            column.read_items(1, _ListReader([]))

    def test_truncated_discriminators_raise(self):
        column = DynamicColumn(_make_getter(self.ctx), context=self.ctx)
        column._shared_variant_index = None
        with self.assertRaises(EOFError):
            # 3 rows promised, only 2 discriminator bytes available.
            column.read_items(3, _ListReader([bytes([0, 0])]))

    def test_out_of_range_discriminator_raises(self):
        column = DynamicColumn(_make_getter(self.ctx), context=self.ctx)
        column._shared_variant_index = None
        with self.assertRaises(ValueError):
            # No variants declared, so any non-NULL discriminator is bogus.
            column.read_items(1, _ListReader([bytes([5])]))

    def test_read_state_prefix_v1_then_all_null_items(self):
        # End-to-end read of a Dynamic(Int64) column header in the legacy
        # V1 framing, followed by a body with only NULL discriminators.
        # Exercises the V1 legacy-slot read, the non-SharedVariant column
        # construction and its state-prefix call, and the
        # "no rows for this variant, skip" branch in read_items.
        buf = _ListReader([
            (1).to_bytes(8, 'little'),    # structure_version = DYNAMIC_V1
            _varint(0),                    # legacy max_dynamic_types slot
            _varint(1),                    # one declared variant
            _binary_string("Int64"),       # variant type spec
            (0).to_bytes(8, 'little'),    # discriminators_mode = BASIC
        ])
        column = DynamicColumn(_make_getter(self.ctx), context=self.ctx)
        column.read_state_prefix(buf)
        self.assertEqual(column.variant_specs, ["Int64"])
        # "Int64" sorts before "SharedVariant", so the latter takes
        # global discriminator 1 and the SharedVariant slot is the last.
        self.assertEqual(column._shared_variant_index, 1)

        result = column.read_items(2, _ListReader([bytes([0xFF, 0xFF])]))
        self.assertEqual(result, [None, None])

    def test_read_items_routes_rows_to_variants(self):
        # Row 0 hits a variant exposing only read_items (the fallback
        # branch); row 1 hits the SharedVariant slot, whose bytes go
        # through the shared-value decoder; row 2 is NULL.
        column = DynamicColumn(_make_getter(self.ctx), context=self.ctx)
        column.discriminators_mode = 0  # VARIANT_MODE_BASIC
        column.variant_columns = [_ReadItemsOnlyColumn(), _ReadDataColumn()]
        column._shared_variant_index = 1
        column.shared_value_decoder = _FakeSharedDecoder()

        result = column.read_items(3, _ListReader([bytes([0, 1, 0xFF])]))
        self.assertEqual(result, ["from_read_items", "decoded:blob", None])


class NewJsonColumnUnitTestCase(TestCase):
    """Branches of the JSON column reader/writer that the live-server
    tests on CH 25.5 do not reach: the string serialization version, and
    a few write-path / normalisation helpers."""

    def setUp(self):
        self.ctx = _make_context()
        self.column = NewJsonColumn(_make_getter(self.ctx), context=self.ctx)

    def test_unsupported_serialization_version_raises(self):
        buf = _SharedValueReader()
        buf.reset((99).to_bytes(8, 'little'))
        with self.assertRaises(NotImplementedError):
            self.column.read_state_prefix(buf)

    def test_string_serialization_round_trip(self):
        # OBJECT_STRING framing: a version header, then one String per row
        # carrying the JSON text. An empty string decodes to ``{}``.
        buf = _SharedValueReader()
        buf.reset(
            (OBJECT_STRING).to_bytes(8, 'little')
            + _binary_string('{"a": 1}')
            + _binary_string("")
        )
        self.column.read_state_prefix(buf)
        self.assertEqual(self.column.serialization_version, OBJECT_STRING)
        self.assertEqual(self.column.read_items(2, buf), [{"a": 1}, {}])

    def test_value_spec_for_mixed_float_and_bool_array(self):
        # A float+bool array has no common numeric type, so it widens to
        # Array(Nullable(String)).
        self.assertEqual(
            self.column._get_json_value_spec([1.5, True], 0),
            "Array(Nullable(String))")

    def test_value_spec_for_pure_bool_array(self):
        self.assertEqual(
            self.column._get_json_value_spec([True, False], 0),
            "Array(Nullable(Bool))")

    def test_preprocess_array_passthrough_for_unknown_type(self):
        # A spec matching none of the scalar branches is passed through
        # unchanged.
        values = [[1, 2]]
        self.assertEqual(
            self.column._preprocess_array(values, "Nothing"), values)

    def test_unfold_json_item_defaults_result(self):
        # Called without an accumulator, it allocates its own.
        result = self.column._unfold_json_item({"a": 1}, depth=0)
        # Leaves are keyed by their dotted path with a trailing separator
        # that ``_unfold_json`` strips later.
        self.assertIn("a.", result)

    def test_denormalize_dotted_paths_conflict_raises(self):
        with self.assertRaises(ValueError):
            self.column._denormalize_dotted_paths({"a": 1, "a.b": 2})

    def test_tuples_to_lists_scalar_and_nested(self):
        self.assertEqual(_tuples_to_lists(5), 5)
        self.assertEqual(_tuples_to_lists((1, "x")), (1, "x"))
        self.assertEqual(_tuples_to_lists([1, 2]), [1, 2])


# ----------------------------------------------------------------------
# Minimal buffer stand-ins
# ----------------------------------------------------------------------

def _SharedValueReader_for(blob):
    reader = _SharedValueReader()
    reader.reset(blob)
    return reader


class _ReadItemsOnlyColumn(object):
    """A variant column that only exposes ``read_items`` — forces
    ``DynamicColumn.read_items`` down its low-level fallback branch."""

    def read_items(self, n_items, buf):
        return ["from_read_items"] * n_items


class _ReadDataColumn(object):
    """The implicit SharedVariant String column: returns raw blobs."""

    def read_data(self, n_items, buf):
        return [b"blob"] * n_items


class _FakeSharedDecoder(object):
    def decode(self, blob):
        return "decoded:" + blob.decode()


class _ListReader(object):
    """A buffer that yields pre-chunked byte strings for the driver's
    ``read_binary_*`` / ``read_varint`` helpers, which only call
    ``read(n)``."""

    def __init__(self, chunks):
        self._buf = b"".join(chunks)
        self._pos = 0

    def read(self, n):
        start = self._pos
        self._pos += n
        return self._buf[start:start + n]

    def read_one(self):
        byte = self._buf[self._pos]
        self._pos += 1
        return byte
