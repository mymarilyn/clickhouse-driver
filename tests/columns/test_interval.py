from tests.testcase import BaseTestCase


class IntervalTestCase(BaseTestCase):

    def test_all(self):
        columns = (
            'a IntervalDay, b IntervalWeek, c IntervalMonth, d IntervalYear, '
            'e IntervalHour, f IntervalMinute, g IntervalSecond'
        )

        data = [
            (-10, -300, -123581321, -123581321345589144,
             10, 300, 123581321, 123581321345589144)
        ]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a, b, c, d, e, f, g, h) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, (
                    '-10\t-300\t-123581321\t-123581321345589144\t'
                    '10\t300\t123581321\t123581321345589144\n'
                )
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
