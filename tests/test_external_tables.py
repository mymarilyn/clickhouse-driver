
from tests.testcase import BaseTestCase


class ExternalTablesTestCase(BaseTestCase):
    def test_select(self):
        tables = [{
            'name': 'test',
            'structure': [('x', 'Int32'), ('y', 'Array(Int32)')],
            'data': [
                {'x': 100, 'y': [2, 4, 6, 8]},
                {'x': 500, 'y': [1, 3, 5, 7]},
            ]
        }]
        rv = self.client.execute('SELECT * FROM test', external_tables=tables)
        self.assertEqual(rv, [(100, (2, 4, 6, 8)), (500, (1, 3, 5, 7))])
