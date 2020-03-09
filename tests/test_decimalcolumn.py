from clickhouse_driver.columns.decimalcolumn import \
    create_decimal_column, Decimal32Column, Decimal64Column, Decimal128Column
from tests.testcase import BaseTestCase

class DecimalColumnTestCase(BaseTestCase):
    def test_create_column_DecimalPS(self):
        col = create_decimal_column(spec='Decimal(18,4)', column_options={})
        self.assertIsInstance(col, Decimal64Column)

    def test_create_column_Decimal_bits_S(self):
        col32 = create_decimal_column(spec='Decimal32(4)', column_options={})
        col64 = create_decimal_column(spec='Decimal64(4)', column_options={})
        col128 = create_decimal_column(spec='Decimal128(4)', column_options={})
        self.assertIsInstance(col32, Decimal32Column)
        self.assertIsInstance(col64, Decimal64Column)
        self.assertIsInstance(col128, Decimal128Column)

    def test_create_column_Decimal_bad(self):
        with self.assertRaises(AssertionError):
            create_decimal_column(spec='Decimal50(4)', column_options={})
