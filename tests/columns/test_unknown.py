
from unittest import TestCase

from clickhouse_driver import errors
from clickhouse_driver.columns.service import get_column_by_spec


class UnknownColumnTestCase(TestCase):
    def test_get_unknown_column(self):
        with self.assertRaises(errors.UnknownTypeError) as e:
            get_column_by_spec('Unicorn', {'context': {}})

        self.assertIn('Unicorn', str(e.exception))
