from tests.testcase import BaseTestCase


class IntervalTestCase(BaseTestCase):

    def test_all(self):
        interval = [
            ('YEAR', 1),
            ('MONTH', 2),
            ('WEEK', 3),
            ('DAY', 4),
            ('HOUR', 5),
            ('MINUTE', 6),
            ('SECOND', 7)
        ]
        columns = ', '.join(['INTERVAL {} {}'.format(v, k)
                             for k, v in interval])
        query = 'SELECT {} FROM system.numbers LIMIT 1'.format(columns)

        cli_result = self.emit_cli(query)
        self.assertEqual(cli_result, '1\t2\t3\t4\t5\t6\t7\n')

        client_result = self.client.execute(query)
        self.assertEqual(client_result, cli_result)
