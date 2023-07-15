from tests.testcase import BaseTestCase


class NestedTestCase(BaseTestCase):
    required_server_version = (21, 3, 13)

    def entuple(self, lst):
        return tuple(
            self.entuple(x) if isinstance(x, list) else x for x in lst
        )

    def test_simple(self):
        columns = 'n Nested(i Int32, s String)'

        # INSERT INTO test_nested VALUES ([(0, 'a'), (1, 'b')]);
        data = [([(0, 'a'), (1, 'b')],)]

        with self.create_table(columns, flatten_nested=0):
            self.client.execute(
                'INSERT INTO test (n) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, "[(0,'a'),(1,'b')]\n")

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

            projected_i = self.client.execute('SELECT n.i FROM test')
            self.assertEqual(
                projected_i,
                [([0, 1],)]
            )

            projected_s = self.client.execute('SELECT n.s FROM test')
            self.assertEqual(
                projected_s,
                [(['a', 'b'],)]
            )

    def test_multiple_rows(self):
        columns = 'n Nested(i Int32, s String)'

        data = [([(0, 'a'), (1, 'b')],), ([(3, 'd'), (4, 'e')],)]

        with self.create_table(columns, flatten_nested=0):
            self.client.execute(
                'INSERT INTO test (n) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "[(0,'a'),(1,'b')]\n[(3,'d'),(4,'e')]\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_dict(self):
        columns = 'n Nested(i Int32, s String)'

        data = [
            {'n': [{'i': 0, 's': 'a'}, {'i': 1, 's': 'b'}]},
            {'n': [{'i': 3, 's': 'd'}, {'i': 4, 's': 'e'}]},
        ]

        with self.create_table(columns, flatten_nested=0):
            self.client.execute(
                'INSERT INTO test (n) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                "[(0,'a'),(1,'b')]\n[(3,'d'),(4,'e')]\n"
            )

            inserted = self.client.execute(query)
            self.assertEqual(
                inserted,
                [([(0, 'a'), (1, 'b')],), ([(3, 'd'), (4, 'e')],)]
            )

    def test_nested_side_effect_as_json(self):
        client_settings = {
            'allow_experimental_object_type': True
        }
        columns = 'n Nested(i Int32, s String)'

        data = [([(0, 'a'), (1, 'b')],)]

        with self.create_table(columns, flatten_nested=0):
            with self.created_client(settings=client_settings) as client:
                client.execute(
                    'INSERT INTO test (n) VALUES', data
                )

            inserted = client.execute('SELECT * FROM test')
            self.assertEqual(
                inserted,
                [([{'i': 0, 's': 'a'}, {'i': 1, 's': 'b'}],)]
            )
