from tests.testcase import BaseTestCase


class SimpleAggregateFunctionTestCase(BaseTestCase):
    required_server_version = (19, 8, 3)

    def test_simple(self):
        columns = 'a SimpleAggregateFunction(any, Int32)'

        data = [(3, ), (2, )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '3\n2\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_nullable(self):
        columns = 'a SimpleAggregateFunction(any, Nullable(Int32))'

        data = [(3, ), (None, ), (2, )]
        with self.create_table(columns):
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted, '3\n\\N\n2\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
