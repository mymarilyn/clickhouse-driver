from time import sleep
from unittest.mock import patch

from tests.testcase import BaseTestCase


class LongInsertTestCase(BaseTestCase):
    client_kwargs = {
        'settings': {
            'insert_block_size': 1,
            'send_timeout': 1,
            'receive_timeout': 1,
        },
    }

    def test_long_insert(self):
        """
        In this test we are trying to emulate the situation, where we have a
        lot of insert blocks. From specific clickhouse version server would
        send ProfileEvents packet in response to each insert.

        This insert should work normally for all clickhouse versions,
        even without response ProfileEvents on each insert.
        The 8_000 rows used to provide somewhat consistent experience of
        bug reproducibility without too long of a test duration.

        `send_timeout` & `receive_timeout` are set to 1,
        so we can emulate the real world situation on synthetic data.
        The server will send exception and timeout if the client will not
        receive the ProfileEvent during this time.

        We modify receive_end_of_query with sleep here to emulate long pause
        before this method calling under normal circumstances.
        """
        original_receive_end_of_query = self.client.receive_end_of_query

        def mocked_receive_end_of_query(*args, **kwargs):
            sleep(2)
            return original_receive_end_of_query(*args, **kwargs)

        with self.create_table('x Int32'):
            with patch.object(
                self.client,
                'receive_end_of_query',
                new=mocked_receive_end_of_query
            ):
                data = [{'x': 1}] * 8_000
                self.client.execute(
                    'INSERT INTO test (x) VALUES', data
                )
