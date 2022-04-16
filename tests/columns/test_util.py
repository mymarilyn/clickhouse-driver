from clickhouse_driver.columns.util import get_inner_spec
from tests.testcase import BaseTestCase


class UtilTestCase(BaseTestCase):
    def test_get_inner_spec(self):
        inner = 'a Tuple(Array(Int8), Array(Int64)), b Nullable(String)'
        self.assertEqual(
            get_inner_spec('Nested', 'Nested({}) dummy '.format(inner)), inner
        )
