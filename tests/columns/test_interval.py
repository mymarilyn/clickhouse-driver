from tests.testcase import BaseTestCase


class IntervalTestCase(BaseTestCase):
    required_server_version = (1, 1, 54310)

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
        query = 'SELECT {}'.format(columns)

        cli_result = self.emit_cli(query)
        self.assertEqual(cli_result, '1\t2\t3\t4\t5\t6\t7\n')

        client_result = self.client.execute(query)
        self.assertEqual(client_result, [(1, 2, 3, 4, 5, 6, 7)])
