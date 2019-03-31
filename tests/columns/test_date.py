import os
from datetime import date, datetime

from freezegun import freeze_time
from mock import patch

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
            self.assertEqual(inserted, testTime.date())

    def test_wrong_datetime_insert(self):
        with self.create_table('a Date'):
            wrongTime = date(5555, 1, 1)
            nullTime = date(1, 1, 1)
            expectedTime = ([date(1970, 1, 1), ] for x in range(2))
            self.client.execute(
                'INSERT INTO test (a) VALUES', [(wrongTime, ), (nullTime, )]
            )
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, expectedTime)
