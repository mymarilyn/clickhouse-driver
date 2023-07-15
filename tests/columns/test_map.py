from tests.testcase import BaseTestCase
from decimal import Decimal


class MapTestCase(BaseTestCase):
    required_server_version = (21, 1, 2)
    stable_support_version = (21, 8, 1)

    def client_kwargs(self, version):
        if version < self.stable_support_version:
            return {'settings': {'allow_experimental_map_type': True}}

    def cli_client_kwargs(self):
        if self.stable_support_version > self.server_version:
            return {'allow_experimental_map_type': 1}

    def _sorted_dicts(self, text):
        items = [
            ('{' + ','.join(sorted(x.strip('{}\n').split(','))) + '}')
            for x in text.strip('\n').split('\n')
        ]
        return '\n'.join(items) + '\n'

    def test_simple(self):
        with self.create_table('a Map(String, UInt64)'):
            data = [
                ({},),
                ({'key1': 1}, ),
                ({'key1': 2, 'key2': 20}, ),
                ({'key1': 3, 'key2': 30, 'key3': 50}, )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                self._sorted_dicts(inserted),
                "{}\n"
                "{'key1':1}\n"
                "{'key1':2,'key2':20}\n"
                "{'key1':3,'key2':30,'key3':50}\n"
            )
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    # def test_nullable(self):
    #     with self.create_table('a Map(Nullable(String), Nullable(UInt64))'):
    #         data = [
    #             ({},),
    #             ({None: None},),
    #             ({'key1': 1}, )
    #         ]
    #         self.client.execute('INSERT INTO test (a) VALUES', data)
    #         query = 'SELECT * FROM test'
    #         inserted = self.emit_cli(query)
    #         self.assertEqual(
    #             inserted,
    #             "{}\n"
    #             "{NULL:NULL}\n"
    #             "{'key1':1}\n"
    #         )
    #         inserted = self.client.execute(query)
    #         self.assertEqual(inserted, data)
    #
    # def test_low_cardinality(self):
    #     columns = 'a Map(LowCardinality(String), LowCardinality(UInt64))'
    #     with self.create_table(columns):
    #         data = [
    #             ({'key1': 1}, ),
    #             ({'key1': 1}, ),
    #             ({'key1': 1}, )
    #         ]
    #         self.client.execute('INSERT INTO test (a) VALUES', data)
    #         query = 'SELECT * FROM test'
    #         inserted = self.emit_cli(query)
    #         self.assertEqual(
    #             inserted,
    #             "{'key1':1}\n"
    #             "{'key1':1}\n"
    #             "{'key1':1}\n"
    #         )
    #         inserted = self.client.execute(query)
    #         self.assertEqual(inserted, data)

    def test_array(self):
        columns = 'a Map(String, Array(UInt64))'
        with self.create_table(columns):
            data = [
                ({'key1': []}, ),
                ({'key2': [1, 2, 3]}, ),
                ({'key3': [1, 1, 1, 1]}, )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "{'key1':[]}\n"
                "{'key2':[1,2,3]}\n"
                "{'key3':[1,1,1,1]}\n"
            )
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_decimal(self):
        columns = 'a Map(String, Decimal(9, 2))'
        with self.create_table(columns):
            data = [
                ({'key1': Decimal('123.45')}, ),
                ({'key2': Decimal('234.56')}, ),
                ({'key3': Decimal('345.67')}, )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "{'key1':123.45}\n"
                "{'key2':234.56}\n"
                "{'key3':345.67}\n"
            )
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
