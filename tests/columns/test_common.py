from clickhouse_driver import Client
from tests.testcase import BaseTestCase


class CommonTestCase(BaseTestCase):
    def test_insert_block_size(self):
        client = Client(
            self.host, self.port, self.database, self.user, self.password,
            settings={'insert_block_size': 1}
        )

        with self.create_table('a UInt8'):
            data = [(x, ) for x in range(4)]
            client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '0\n1\n2\n3\n')
            inserted = client.execute(query)
            self.assertEqual(inserted, data)

        client.disconnect()
