from clickhouse_driver import errors

try:
    from clickhouse_driver.columns.numpy.service import \
        get_numpy_column_by_spec
except ImportError:
    get_numpy_column_by_spec = None

from clickhouse_driver.context import Context

from tests.numpy.testcase import NumpyBaseTestCase


class OtherColumnsTestCase(NumpyBaseTestCase):
    def get_column(self, spec):
        ctx = Context()
        ctx.client_settings = {'strings_as_bytes': False}
        return get_numpy_column_by_spec(spec, {'context': ctx})

    def test_enum(self):
        col = self.get_column("Enum8('hello' = 1, 'world' = 2)")
        self.assertIsNotNone(col)

    def test_decimal(self):
        col = self.get_column('Decimal(8, 4)')
        self.assertIsNotNone(col)

    def test_array(self):
        col = self.get_column('Array(String)')
        self.assertIsNotNone(col)

    def test_tuple(self):
        col = self.get_column('Tuple(String)')
        self.assertIsNotNone(col)

    def test_simple_aggregation_function(self):
        col = self.get_column('SimpleAggregateFunction(any, Int32)')
        self.assertIsNotNone(col)

    def test_get_unknown_column(self):
        with self.assertRaises(errors.UnknownTypeError) as e:
            self.get_column('Unicorn')

        self.assertIn('Unicorn', str(e.exception))
