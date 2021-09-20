import os
from datetime import date, datetime
from unittest.mock import patch

from freezegun import freeze_time

from tests.testcase import BaseTestCase


class DateTestCase(BaseTestCase):
    @freeze_time('2017-03-05 03:00:00')
    def test_do_not_use_timezone(self):
        with self.create_table('a Date'):
            data = [(date(1970, 1, 2), )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1970-01-02\n')

            with patch.dict(os.environ, {'TZ': 'US/Hawaii'}):
                inserted = self.client.execute(query)
                self.assertEqual(inserted, data)

    def test_insert_datetime_to_date(self):
        with self.create_table('a Date'):
            testTime = datetime(2015, 6, 6, 12, 30, 54)
            self.client.execute(
                'INSERT INTO test (a) VALUES', [(testTime, )]
            )
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '2015-06-06\n')

    def test_wrong_date_insert(self):
        with self.create_table('a Date'):
            data = [
                (date(5555, 1, 1), ),
                (date(1, 1, 1), ),
                (date(2149, 6, 7), )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            expected = (
                3*'1970-01-01\n' if self.server_version > (20, 7, 2)
                else 3*'0000-00-00\n'
            )
            self.assertEqual(inserted, expected)

    def test_boundaries(self):
        extended_date = self.server_version > (21, 4)

        with self.create_table('a Date'):
            data = [
                (date(1970, 1, 1), ),
                ((date(2149, 6, 6) if extended_date else date(2106, 2, 7)), )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            if extended_date:
                expected = '1970-01-01\n2149-06-06\n'
            else:
                if self.server_version > (20, 7, 2):
                    expected = '1970-01-01\n2106-02-07\n'
                else:
                    expected = '0000-00-00\n2106-02-07\n'
            self.assertEqual(inserted, expected)

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)


class Date32TestCase(BaseTestCase):
    required_server_version = (21, 9)

    def test_wrong_date_insert(self):
        with self.create_table('a Date32'):
            data = [
                (date(5555, 1, 1), ),
                (date(1, 1, 1), ),
                (date(2284, 1, 1), )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1970-01-01\n1970-01-01\n1970-01-01\n')

    def test_boundaries(self):
        with self.create_table('a Date32'):
            data = [(date(1925, 1, 1), ), (date(2283, 11, 11), )]
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '1925-01-01\n2283-11-11\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
