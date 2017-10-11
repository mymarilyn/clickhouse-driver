from src.errors import ServerException
from tests.testcase import BaseTestCase


class BlocksTestCase(BaseTestCase):

    def test_return_totals_extremes(self):
        rv = self.client.execute(
            'SELECT a, sum(b + a) FROM ('
            'SELECT arrayJoin(range(3)) - 1 AS a,'
            'arrayJoin(range(4)) AS b'
            ') AS t '
            'GROUP BY a WITH TOTALS '
            'ORDER BY a',
            settings={'extremes': 1}
        )
        self.assertEqual(rv, [
            (-1, 2),
            (0, 6),
            (1, 10),

            # TOTALS
            (0, 18),

            # EXTREMES
            (-1, 2),
            (1, 10)
        ])

    def test_columnar_result(self):
        rv = self.client.execute(
            'SELECT a, sum(b + a) FROM ('
            'SELECT arrayJoin(range(3)) - 1 AS a,'
            'arrayJoin(range(4)) AS b'
            ') AS t '
            'GROUP BY a '
            'ORDER BY a',
            columnar=True
        )
        self.assertEqual(rv, [
            (-1, 0, 1),
            (2, 6, 10)
        ])

    def test_select_with_column_types(self):
        rv = self.client.execute(
            'SELECT CAST(1 AS Int32) AS x', with_column_types=True
        )
        self.assertEqual(rv, ([(1,)], [('x', 'Int32')]))


class ProgressTestCase(BaseTestCase):
    def test_select_with_progress(self):
        progress = self.client.execute_with_progress('SELECT 2')
        self.assertEqual(list(progress), [(1, 0)])
        self.assertEqual(progress.get_result(), [(2,)])

    def test_select_with_progress_error(self):
        with self.assertRaises(ServerException):
            progress = self.client.execute_with_progress('SELECT error')
            list(progress)

    def test_select_with_progress_no_progress_unwind(self):
        progress = self.client.execute_with_progress('SELECT 2')
        self.assertEqual(progress.get_result(), [(2,)])

    def test_select_with_progress_cancel(self):
        self.client.execute_with_progress('SELECT 2')
        rv = self.client.cancel()
        self.assertEqual(rv, [(2,)])

    def test_select_with_progress_cancel_with_column_types(self):
        self.client.execute_with_progress('SELECT CAST(2 AS Int32) as x')
        rv = self.client.cancel(with_column_types=True)
        self.assertEqual(rv, ([(2,)], [('x', 'Int32')]))
