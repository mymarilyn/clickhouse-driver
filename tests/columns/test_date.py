import os
from datetime import date

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
