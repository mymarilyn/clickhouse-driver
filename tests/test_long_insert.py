from tests.testcase import BaseTestCase


class LongInsertTestCase(BaseTestCase):
    client_kwargs = {
        'settings': {
            'insert_block_size': 1,
            'send_timeout': 1,
            'receive_timeout': 1,
        },
        'send_receive_timeout': 2,
    }

    def test_long_insert(self):
        data = [{'x': 1}] * 100_000
        self.client.execute(
            'insert into function null(\'x Int\') (x) values',
            data
        )


