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

    def test_select_with_column_types(self):
        rv = self.client.execute(
            'SELECT CAST(1 AS Int32) AS x', with_column_types=True
        )
        self.assertEqual(rv, ([(1,)], [('x', 'Int32')]))
