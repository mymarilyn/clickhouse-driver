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
        The 100_000 rows used to provide somewaht consistent experience of
        bug reproducability without too long test duration.

        `send_timeout` & `receive_timeout` are set to 1,
        so we can emulate the real world situation on synthetic data.
        The server will send exception and timeout if the client will not
        receive the ProfileEvent during this time.
        """
        with self.create_table('x Int32'):
            data = [{'x': 1}] * 100_000
            self.client.execute(
                'INSERT INTO test (x) VALUES', data
            )
