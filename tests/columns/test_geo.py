from tests.testcase import BaseTestCase


class GeoTypesTestCase(BaseTestCase):
    required_server_version = (20, 5, 1)

    def client_kwargs(self, version):
        return {'settings': {'allow_experimental_geo_types': True}}

    def cli_client_kwargs(self):
        return {'allow_experimental_geo_types': 1}

    def entuple(self, lst):
        return tuple(
            self.entuple(x) if isinstance(x, list) else x for x in lst
        )

    def test_point(self):
        columns = 'a Point'
        data = [
            ((1.5, 2), ),
            ((3, 4), )
        ]

        with self.create_table(columns):
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '(1.5,2)\n(3,4)\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_ring(self):
        columns = 'a Ring'
        data = [
            ([(1.5, 2), (3, 4)], )
        ]

        with self.create_table(columns):
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '[(1.5,2),(3,4)]\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_polygon(self):
        columns = 'a Polygon'
        data = [
            ([
                [(1.5, 2), (3, 4)],
                [(5.5, 6), (7, 8)],
             ], )
        ]

        with self.create_table(columns):
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, '[[(1.5,2),(3,4)],[(5.5,6),(7,8)]]\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)

    def test_multipolygon(self):
        columns = 'a MultiPolygon'
        data = [
            ([
                [
                    [(1.5, 2), (3, 4)],
                    [(5.5, 6), (7, 8)],
                ],
                [
                    [(2.5, 3), (4, 5)],
                    [(6.5, 7), (8, 9)],
                ]
             ], )
        ]

        with self.create_table(columns):
            self.client.execute('INSERT INTO test (a) VALUES', data)

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(
                inserted,
                '['
                '[[(1.5,2),(3,4)],[(5.5,6),(7,8)]],'
                '[[(2.5,3),(4,5)],[(6.5,7),(8,9)]]'
                ']\n'
            )

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
