import json

from tests.testcase import BaseTestCase


class JSONTestCase(BaseTestCase):
    required_server_version = (22, 3, 2)

    def client_kwargs(self, version):
        return {'settings': {'allow_experimental_object_type': True}}

    def cli_client_kwargs(self):
        return {'allow_experimental_object_type': 1}

    def test_simple(self):
        rv = self.client.execute("SELECT '{\"bb\": {\"cc\": [255, 1]}}'::JSON")
        self.assertEqual(rv, [({'bb': {'cc': [255, 1]}},)])

    def test_from_table(self):
        with self.create_table('a JSON'):
            data = [
                ({},),
                ({'key1': 1}, ),
                ({'key1': 2.1, 'key2': {'nested': 'key'}}, ),
                ({'key1': 3, 'key3': ['test'], 'key4': [10, 20]}, )
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "(0,(''),[],[])\n"
                "(1,(''),[],[])\n"
                "(2.1,('key'),[],[])\n"
                "(3,(''),['test'],[10,20])\n"
            )
            inserted = self.client.execute(query)
            data_with_all_keys = [
                ({'key1': 0, 'key2': {'nested': ''}, 'key3': [], 'key4': []},),
                ({'key1': 1, 'key2': {'nested': ''}, 'key3': [], 'key4': []},),
                ({'key1': 2.1, 'key2': {'nested': 'key'}, 'key3': [],
                  'key4': []},),
                ({'key1': 3, 'key2': {'nested': ''}, 'key3': ['test'],
                  'key4': [10, 20]},)
            ]
            self.assertEqual(inserted, data_with_all_keys)

    def test_insert_json_strings(self):
        with self.create_table('a JSON'):
            data = [
                (json.dumps({'i-am': 'dumped json'}),),
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "('dumped json')\n"
            )
            inserted = self.client.execute(query)
            data_with_all_keys = [
                ({'`i-am`': 'dumped json'},)
            ]
            self.assertEqual(inserted, data_with_all_keys)

    def test_json_as_named_tuple(self):
        settings = {'namedtuple_as_json': False}
        query = 'SELECT * FROM test'

        with self.create_table('a JSON'):
            data = [
                ({'key': 'value'}, ),
            ]
            self.client.execute('INSERT INTO test (a) VALUES', data)
            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

            with self.created_client(settings=settings) as client:
                inserted = client.execute(query)
                self.assertEqual(inserted, [(('value',),)])
