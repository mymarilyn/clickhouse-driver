try:
    import pyarrow as pa
except ImportError:
    pa = None

from tests.testcase import BaseTestCase


class ArrowBaseTestCase(BaseTestCase):
    def setUp(self):
        if pa is None:
            self.skipTest('PyArrow package is not installed')
        super(ArrowBaseTestCase, self).setUp()

    def assert_arrow_column(self, ch_type, expected_pa_type, data,
                            expected=None):
        """
        Creates a table with a single column of ``ch_type``, inserts
        ``data`` into it and checks that ``query_arrow`` returns a table
        with expected Arrow type and values.
        """
        with self.create_table('a {}'.format(ch_type)):
            self.client.execute(
                'INSERT INTO test (a) VALUES', [(x, ) for x in data]
            )
            table = self.client.query_arrow('SELECT a FROM test')

            self.assertIsInstance(table, pa.Table)
            self.assertEqual(table.schema.field('a').type, expected_pa_type)
            self.assertEqual(
                table.column('a').to_pylist(),
                data if expected is None else expected
            )
