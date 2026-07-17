from unittest import TestCase
from uuid import UUID

from clickhouse_driver.context import Context

try:
    import pyarrow as pa

    from clickhouse_driver.arrow import mapping
    from clickhouse_driver.arrow.convert import (
        ArrowStreamState, _json_declared_converter,
        create_record_batch_reader
    )
except ImportError:
    pa = None
    mapping = None


class MappingTestCase(TestCase):
    """
    Unit tests for the spec -> Arrow type mapping. No server needed.
    """

    def setUp(self):
        if pa is None:
            self.skipTest('PyArrow package is not installed')

    def get(self, spec, **kwargs):
        return mapping.get_type_and_converter(spec, **kwargs)

    def test_object_json_unsupported(self):
        # Legacy JSON type: only served by pre-25 servers.
        type_, _ = self.get("Object('json')")
        self.assertIs(type_, mapping.UNSUPPORTED)

    def test_decimal_prefixed_specs(self):
        self.assertEqual(self.get('Decimal32(2)')[0], pa.decimal128(9, 2))
        self.assertEqual(self.get('Decimal64(4)')[0], pa.decimal128(18, 4))
        self.assertEqual(self.get('Decimal128(6)')[0], pa.decimal128(38, 6))
        self.assertEqual(self.get('Decimal256(8)')[0], pa.decimal256(76, 8))

    def test_unknown_decimal_prefix_falls_back_to_inference(self):
        self.assertEqual(self.get('Decimal512(2)'), (None, None))

    def test_decimal_high_precision_is_decimal256(self):
        self.assertEqual(self.get('Decimal(76, 10)')[0],
                         pa.decimal256(76, 10))

    def test_datetime64_units(self):
        self.assertEqual(self.get('DateTime64(0)')[0], pa.timestamp('s'))
        self.assertEqual(self.get('DateTime64(9)')[0], pa.timestamp('ns'))

    def test_tuple_without_json_falls_back_to_inference(self):
        self.assertEqual(self.get('Tuple(UInt8, String)'), (None, None))

    def test_unknown_spec_falls_back_to_inference(self):
        self.assertEqual(self.get('Int128'), (None, None))

    def test_simple_aggregate_function_unwraps(self):
        type_, converter = self.get('SimpleAggregateFunction(anyLast, Int64)')
        self.assertEqual(type_, pa.int64())
        self.assertIsNone(converter)

    def test_array_of_uuid_converter(self):
        type_, converter = self.get('Array(UUID)')
        self.assertEqual(type_, pa.list_(pa.string()))

        uid = UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d')
        self.assertEqual(converter([uid, None]), [str(uid), None])

    def test_map_with_value_converter(self):
        type_, converter = self.get('Map(String, UUID)')
        self.assertEqual(type_, pa.map_(pa.string(), pa.string()))

        uid = UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d')
        self.assertEqual(converter({'k': uid}), [('k', str(uid))])

    def test_json_as_object_parses_wire_strings(self):
        self.assertEqual(mapping.json_as_object('{"a": 1}'), {'a': 1})
        self.assertEqual(mapping.json_as_object(''), {})
        self.assertEqual(mapping.json_as_object({'a': 1}), {'a': 1})


class ConvertTestCase(TestCase):
    """
    Unit tests for reader creation edge cases. No server needed.
    """

    def setUp(self):
        if pa is None:
            self.skipTest('PyArrow package is not installed')

    def make_context(self):
        context = Context()
        context.client_settings = {'strings_as_bytes': False}
        return context

    def test_empty_packet_stream(self):
        state = ArrowStreamState(connection=None)
        reader = create_record_batch_reader(
            iter([]), self.make_context(), state=state
        )

        self.assertEqual(reader.read_all().num_rows, 0)
        self.assertTrue(state.finished)

    def test_declared_converter_none_for_json_free_containers(self):
        self.assertIsNone(_json_declared_converter(
            'Array(Int32)', pa.list_(pa.int32()), 'x'
        ))
        self.assertIsNone(_json_declared_converter(
            'Map(String, Int32)', pa.map_(pa.string(), pa.int32()), 'x'
        ))
