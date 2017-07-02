from datetime import date, datetime

from tests.testcase import BaseTestCase


class DatetimeTestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a Date, b DateTime'):
            data = [(date(2012, 10, 25), datetime(2012, 10, 25, 14, 7, 19))]
            self.client.execute(
                'INSERT INTO test (a, b) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2012-10-25\t2012-10-25 14:07:19\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_date(self):
        with self.create_table('a Nullable(Date)'):
            data = [
                (None, ), (date(2012, 10, 25), ),
                (None, ), (date(2017, 6, 23), )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '\\N\n2012-10-25\n\\N\n2017-06-23\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable_datetime(self):
        with self.create_table('a Nullable(DateTime)'):
            data = [
                (None, ), (datetime(2012, 10, 25, 14, 7, 19), ),
                (None, ), (datetime(2017, 6, 23, 19, 10, 15), )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '\\N\n2012-10-25 14:07:19\n\\N\n2017-06-23 19:10:15\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
