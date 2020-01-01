from tests.testcase import BaseTestCase


class CommonTestCase(BaseTestCase):
    client_kwargs = {'settings': {'insert_block_size': 1}}

    def setUp(self):
        super(CommonTestCase, self).setUp()

        self.send_data_count = 0
        old_send_data = self.client.connection.send_data

        def send_data(*args, **kwargs):
            self.send_data_count += 1
            return old_send_data(*args, **kwargs)

        self.client.connection.send_data = send_data

    def test_insert_block_size(self):
        with self.create_table('a UInt8'):
            data = [(x, ) for x in range(4)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )
            # Two empty blocks: for end of sending external tables
            # and data.
            self.assertEqual(self.send_data_count, 4 + 2)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '0\n1\n2\n3\n')
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_columnar_insert_block_size(self):
        with self.create_table('a UInt8'):
            data = [(0, 1, 2, 3)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, columnar=True
            )
            # Two empty blocks: for end of sending external tables
            # and data.
            self.assertEqual(self.send_data_count, 4 + 2)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '0\n1\n2\n3\n')
            inserted = self.client.execute(query)
            expected = [(0, ), (1, ), (2, ), (3, )]
            self.assertEqual(inserted, expected)
