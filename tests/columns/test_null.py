from tests.testcase import BaseTestCase


class NullTestCase(BaseTestCase):
    def test_select_null(self):
        rv = self.client.execute('SELECT NULL')
        self.assertEqual(rv, [(None, )])
