from tests.testcase import BaseTestCase


class NewJSONTestCase(BaseTestCase):
    required_server_version = (24, 8, 0)

    def client_kwargs(self, version):
        return {"settings": {"enable_json_type": True}}

    def cli_client_kwargs(self):
        return {"enable_json_type": 1}

    def test_simple(self):
        rv = self.client.execute('SELECT \'{"bb": {"cc": [255, 1]}}\'::JSON')
        self.assertEqual(rv, [({"bb": {"cc": [255, 1]}},)])

    def test_json_0(self):
        with self.create_table("a JSON"):
            data = [({"key": 1},), ({"key": "val"},), ({"key": 2.0},)]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({"key": 1},), ({"key": "val"},), ({"key": 2.0},)]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_0_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"key": 1}\'),\
                     (\'{"key": "val"}\'),\
                     (\'{"key": 2.0}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({"key": 1},), ({"key": "val"},), ({"key": 2.0},)]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_1(self):
        with self.create_table("a JSON"):
            data = [
                (
                    {
                        "user_id": 101,
                        "username": "john_doe",
                        "email": "john.doe@example.com",
                        "profile": {
                            "first_name": "John",
                            "last_name": "Doe",
                            "age": 30,
                            "gender": "male",
                        },
                        "preferences": {"theme": "dark", "notifications": True},
                        "roles": ["admin", "user"],
                    },
                )
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                (
                    {
                        "email": "john.doe@example.com",
                        "roles": ["admin", "user"],
                        "user_id": 101,
                        "username": "john_doe",
                        "preferences": {"notifications": True, "theme": "dark"},
                        "profile": {
                            "age": 30,
                            "first_name": "John",
                            "gender": "male",
                            "last_name": "Doe",
                        },
                    },
                )
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_1_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"user_id": 101, "username": "john_doe", "email": "john.doe@example.com", "profile": {"first_name": "John", "last_name": "Doe", "age": 30, "gender": "male"}, "preferences": {"theme": "dark", "notifications": true}, "roles": ["admin", "user"]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                (
                    {
                        "email": "john.doe@example.com",
                        "roles": ["admin", "user"],
                        "user_id": 101,
                        "username": "john_doe",
                        "preferences": {"notifications": True, "theme": "dark"},
                        "profile": {
                            "age": 30,
                            "first_name": "John",
                            "gender": "male",
                            "last_name": "Doe",
                        },
                    },
                )
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_2(self):
        with self.create_table("a JSON"):
            data = [
                ({},),
                ({"foo": "bar", "bar": "baz"},),
                ({"baz": "qux", "foo": 4919},),
                ({"qux": "quux"},),
                ({"foo": "AAAA"},),
                ({"qux": 14099},),
                ({"foo": [1, 0.2, "bar", "baz", False]},),
                ({"foo": 0.1337},),
                ({"foo": False},),
                ({"bar": 1337},),
                ({"bar": 0.999},),
                ({"quux": 1000},),
                ({"quux": 2000},),
                ({"quux": 20.25},),
                ({"alice": 0.432},),
                ({"bob": 0.991},),
                ({"boolean": True},),
                ({"null": None},),
                ({"string": "A quick brown fox jumps over the lazy dog."},),
                (
                    {
                        "nested": {
                            "number": 4141,
                            "string": "Hello, World!",
                            "double-nested": {
                                "foo": "bar",
                                "no.escaping": "1337",
                                "triple-nested": {"foo": "bar"},
                                "numbers": [1, 2, 3],
                                "floats": [0.1, 0.2, 4],
                                "tuple-list": [
                                    1,
                                    3,
                                    "asdf",
                                    [1, 4, 6],
                                    {"foo": "bar", "list": [
                                        1, 2, {"hello": "world"}]},
                                ],
                            },
                        }
                    },
                ),
                (
                    {
                        "list": [
                            123,
                            "2",
                            True,
                            {"foo": "bar", "list": [0.123, {"baz": "bar"}]},
                        ]
                    },
                ),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({},),
                ({"bar": "baz", "foo": "bar"},),
                ({"baz": "qux", "foo": 4919},),
                ({"qux": "quux"},),
                ({"foo": "AAAA"},),
                ({"qux": 14099},),
                ({"foo": ["1", "0.2", "bar", "baz", "false"]},),
                ({"foo": 0.1337},),
                ({"foo": False},),
                ({"bar": 1337},),
                ({"bar": 0.999},),
                ({"quux": 1000},),
                ({"quux": 2000},),
                ({"quux": 20.25},),
                ({"alice": 0.432},),
                ({"bob": 0.991},),
                ({"boolean": True},),
                ({},),
                ({"string": "A quick brown fox jumps over the lazy dog."},),
                (
                    {
                        "nested": {
                            "double-nested": {
                                "floats": [0.1, 0.2, 4.0],
                                "foo": "bar",
                                "no": {"escaping": "1337"},
                                "numbers": [1, 2, 3],
                                "triple-nested": {"foo": "bar"},
                                "tuple-list": [
                                    1,
                                    3,
                                    "asdf",
                                    [1, 4, 6],
                                    {"foo": "bar", "list": [
                                        1, 2, {"hello": "world"}]},
                                ],
                            },
                            "number": 4141,
                            "string": "Hello, World!",
                        }
                    },
                ),
                (
                    {
                        "list": [
                            123,
                            "2",
                            True,
                            {"foo": "bar", "list": [0.123, {"baz": "bar"}]},
                        ]
                    },
                ),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_2_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{}\'),\
                     (\'{"foo": "bar", "bar": "baz"}\'),\
                     (\'{"baz": "qux", "foo": 4919}\'),\
                     (\'{"qux": "quux"}\'),\
                     (\'{"foo": "AAAA"}\'),\
                     (\'{"qux": 14099}\'),\
                     (\'{"foo": [1, 0.2, "bar", "baz", false]}\'),\
                     (\'{"foo": 0.1337}\'),\
                     (\'{"foo": false}\'),\
                     (\'{"bar": 1337}\'),\
                     (\'{"bar": 0.999}\'),\
                     (\'{"quux": 1000}\'),\
                     (\'{"quux": 2000}\'),\
                     (\'{"quux": 20.25}\'),\
                     (\'{"alice": 0.432}\'),\
                     (\'{"bob": 0.991}\'),\
                     (\'{"boolean": true}\'),\
                     (\'{"null": null}\'),\
                     (\'{"string": "A quick brown fox jumps over the lazy dog."}\'),\
                     (\'{"nested": {"number": 4141, "string": "Hello, World!", "double-nested": {"foo": "bar", "no.escaping": "1337", "triple-nested": {"foo": "bar"}, "numbers": [1, 2, 3], "floats": [0.1, 0.2, 4], "tuple-list": [1, 3, "asdf", [1, 4, 6], {"foo": "bar", "list": [1, 2, {"hello": "world"}]}]}}}\'),\
                     (\'{"list": [123, "2", true, {"foo": "bar", "list": [0.123, {"baz": "bar"}]}]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({},),
                ({"bar": "baz", "foo": "bar"},),
                ({"baz": "qux", "foo": 4919},),
                ({"qux": "quux"},),
                ({"foo": "AAAA"},),
                ({"qux": 14099},),
                ({"foo": ["1", "0.2", "bar", "baz", "false"]},),
                ({"foo": 0.1337},),
                ({"foo": False},),
                ({"bar": 1337},),
                ({"bar": 0.999},),
                ({"quux": 1000},),
                ({"quux": 2000},),
                ({"quux": 20.25},),
                ({"alice": 0.432},),
                ({"bob": 0.991},),
                ({"boolean": True},),
                ({},),
                ({"string": "A quick brown fox jumps over the lazy dog."},),
                (
                    {
                        "nested": {
                            "double-nested": {
                                "floats": [0.1, 0.2, 4.0],
                                "foo": "bar",
                                "no": {"escaping": "1337"},
                                "numbers": [1, 2, 3],
                                "triple-nested": {"foo": "bar"},
                                "tuple-list": [
                                    1,
                                    3,
                                    "asdf",
                                    [1, 4, 6],
                                    {"foo": "bar", "list": [
                                        1, 2, {"hello": "world"}]},
                                ],
                            },
                            "number": 4141,
                            "string": "Hello, World!",
                        }
                    },
                ),
                (
                    {
                        "list": [
                            123,
                            "2",
                            True,
                            {"foo": "bar", "list": [0.123, {"baz": "bar"}]},
                        ]
                    },
                ),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_3(self):
        with self.create_table("a JSON"):
            data = [
                ({"list": [1, "asdf", 0.025, None,
                 ["foo", "bar"], ["foo", "bar"]]},)
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({"list": (1, "asdf", 0.025, None,
                 ["foo", "bar"], ["foo", "bar"])},)
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_3_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"list": [1, "asdf", 0.025, null, ["foo", "bar"], ["foo", "bar"]]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({"list": (1, "asdf", 0.025, None,
                 ["foo", "bar"], ["foo", "bar"])},)
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_4(self):
        with self.create_table("a JSON"):
            data = [
                ({"list": [1, 2, 3]},),
                ({"list": [0.1, 2, 3, None]},),
                ({"list": ["test"]},),
                ({"list": [None, None, None]},),
                ({"list": [1, 2, None]},),
                ({"list": ["asdf", None]},),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({"list": [1, 2, 3]},),
                ({"list": [0.1, 2.0, 3.0, 0.0]},),
                ({"list": ["test"]},),
                ({"list": [None, None, None]},),
                ({"list": [1, 2, 0]},),
                ({"list": ["asdf", None]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_4_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"list": [1, 2, 3]}\'),\
                     (\'{"list": [0.1, 2, 3, null]}\'),\
                     (\'{"list": ["test"]}\'),\
                     (\'{"list": [null, null, null]}\'),\
                     (\'{"list": [1, 2, null]}\'),\
                     (\'{"list": ["asdf", null]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({"list": [1, 2, 3]},),
                ({"list": [0.1, 2.0, 3.0, None]},),
                ({"list": ["test"]},),
                ({"list": [None, None, None]},),
                ({"list": [1, 2, None]},),
                ({"list": ["asdf", None]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_5(self):
        with self.create_table("a JSON"):
            data = [
                (
                    {
                        "list": [
                            "123",
                            "456",
                            {
                                "nested-list": [
                                    "789",
                                    "10",
                                    {
                                        "double-nested-list": [
                                            14099,
                                            "AAAA",
                                            {
                                                "triple-nested-list": [
                                                    1,
                                                    2,
                                                    {
                                                        "quadruple-nested-list": [
                                                            3,
                                                            4,
                                                            {
                                                                "quintuple-nested-list": [
                                                                    5,
                                                                    6,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                )
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                (
                    {
                        "list": [
                            "123",
                            "456",
                            {
                                "nested-list": [
                                    "789",
                                    "10",
                                    {
                                        "double-nested-list": [
                                            14099,
                                            "AAAA",
                                            {
                                                "triple-nested-list": [
                                                    1,
                                                    2,
                                                    {
                                                        "quadruple-nested-list": [
                                                            3,
                                                            4,
                                                            {
                                                                "quintuple-nested-list": [
                                                                    5,
                                                                    6,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                )
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_5_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"list": ["123", "456", {"nested-list": ["789", "10", {"double-nested-list": [14099, "AAAA", {"triple-nested-list": [1, 2, {"quadruple-nested-list": [3, 4, {"quintuple-nested-list": [5, 6]}]}]}]}]}]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                (
                    {
                        "list": [
                            "123",
                            "456",
                            {
                                "nested-list": [
                                    "789",
                                    "10",
                                    {
                                        "double-nested-list": [
                                            14099,
                                            "AAAA",
                                            {
                                                "triple-nested-list": [
                                                    1,
                                                    2,
                                                    {
                                                        "quadruple-nested-list": [
                                                            3,
                                                            4,
                                                            {
                                                                "quintuple-nested-list": [
                                                                    5,
                                                                    6,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                )
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_6(self):
        with self.create_table("a JSON"):
            data = [
                (
                    {
                        "list": [
                            "123",
                            "456",
                            {"nested-list": ["789", "10",
                                             {"double-nested": "test"}]},
                        ]
                    },
                ),
                (
                    {
                        "list": [
                            "1337",
                            "444",
                            {"nested-list": ["123", "654", {"asdf": "fdas"}]},
                        ]
                    },
                ),
                ({"list": ["123", "456", {"nested-list": "1234"}]},),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                (
                    {
                        "list": [
                            "123",
                            "456",
                            {"nested-list": ["789", "10",
                                             {"double-nested": "test"}]},
                        ]
                    },
                ),
                (
                    {
                        "list": [
                            "1337",
                            "444",
                            {"nested-list": ["123", "654", {"asdf": "fdas"}]},
                        ]
                    },
                ),
                ({"list": ["123", "456", {"nested-list": "1234"}]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_6_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"list": ["123", "456", {"nested-list": ["789", "10", {"double-nested": "test"}]}]}\'),\
                     (\'{"list": ["1337", "444", {"nested-list": ["123", "654", {"asdf": "fdas"}]}]}\'),\
                     (\'{"list": ["123", "456", {"nested-list": "1234"}]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                (
                    {
                        "list": [
                            "123",
                            "456",
                            {"nested-list": ["789", "10",
                                             {"double-nested": "test"}]},
                        ]
                    },
                ),
                (
                    {
                        "list": [
                            "1337",
                            "444",
                            {"nested-list": ["123", "654", {"asdf": "fdas"}]},
                        ]
                    },
                ),
                ({"list": ["123", "456", {"nested-list": "1234"}]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_7(self):
        with self.create_table("a JSON"):
            data = [
                ({"list": [1, "2", {"foo": "bar"}]},),
                ({"list": "not a list"},),
                ({"list": 14009},),
                ({"list": 0.025},),
                ({"list": True},),
                ({"list": [14099, {"bar": "baz"}, {"baz": "quux"}]},),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({"list": [1, "2", {"foo": "bar"}]},),
                ({"list": "not a list"},),
                ({"list": 14009},),
                ({"list": 0.025},),
                ({"list": True},),
                ({"list": [14099, {"bar": "baz"}, {"baz": "quux"}]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_7_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"list": [1, "2", {"foo": "bar"}]}\'),\
                     (\'{"list": "not a list"}\'),\
                     (\'{"list": 14009}\'),\
                     (\'{"list": 0.025}\'),\
                     (\'{"list": true}\'),\
                     (\'{"list": [14099, {"bar": "baz"}, {"baz": "quux"}]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({"list": [1, "2", {"foo": "bar"}]},),
                ({"list": "not a list"},),
                ({"list": 14009},),
                ({"list": 0.025},),
                ({"list": True},),
                ({"list": [14099, {"bar": "baz"}, {"baz": "quux"}]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_8(self):
        with self.create_table("a JSON"):
            data = [
                ({"list1": [1, "2", {"foo": "bar"}]},),
                ({"string": "string"},),
                ({"int": 14009},),
                ({"float": 0.025},),
                ({"bool": True},),
                ({"list2": [14099, {"bar": "baz"}, {"baz": "quux"}]},),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({"list1": [1, "2", {"foo": "bar"}]},),
                ({"string": "string"},),
                ({"int": 14009},),
                ({"float": 0.025},),
                ({"bool": True},),
                ({"list2": [14099, {"bar": "baz"}, {"baz": "quux"}]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_8_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"list1": [1, "2", {"foo": "bar"}]}\'),\
                     (\'{"string": "string"}\'),\
                     (\'{"int": 14009}\'),\
                     (\'{"float": 0.025}\'),\
                     (\'{"bool": true}\'),\
                     (\'{"list2": [14099, {"bar": "baz"}, {"baz": "quux"}]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({"list1": [1, "2", {"foo": "bar"}]},),
                ({"string": "string"},),
                ({"int": 14009},),
                ({"float": 0.025},),
                ({"bool": True},),
                ({"list2": [14099, {"bar": "baz"}, {"baz": "quux"}]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_9(self):
        with self.create_table("a JSON"):
            data = [
                ({"list": [1, "asdf", 0.025, ["foo", "bar", ["baz", "qux"]]]},),
                ({"list": [10, "fdas", 0.075, ["bar", "foo", ["qux", "baz"]]]},),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({"list": (1, "asdf", 0.025,
                 ("foo", "bar", ["baz", "qux"]))},),
                ({"list": (10, "fdas", 0.075,
                 ("bar", "foo", ["qux", "baz"]))},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_9_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"list": [1, "asdf", 0.025, ["foo", "bar", ["baz", "qux"]]]}\'),\
                     (\'{"list": [10, "fdas", 0.075, ["bar", "foo", ["qux", "baz"]]]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({"list": (1, "asdf", 0.025,
                 ("foo", "bar", ["baz", "qux"]))},),
                ({"list": (10, "fdas", 0.075,
                 ("bar", "foo", ["qux", "baz"]))},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_10(self):
        with self.create_table("a JSON"):
            data = [({"list": ["a", None, "b", None, "c", None]},)]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [({"list": ["a", None, "b", None, "c", None]},)]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_10_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"list": ["a", null, "b", null, "c", null]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [({"list": ["a", None, "b", None, "c", None]},)]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_11(self):
        with self.create_table("a JSON"):
            data = [
                ({"asdf": [{"foo": "bar"}, {"bar": "baz"}]},),
                ({"asdf": [{"baz": "qux"}, {"qux": "quux"}]},),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({"asdf": [{"foo": "bar"}, {"bar": "baz"}]},),
                ({"asdf": [{"baz": "qux"}, {"qux": "quux"}]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_11_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"asdf": [{"foo": "bar"}, {"bar": "baz"}]}\'),\
                     (\'{"asdf": [{"baz": "qux"}, {"qux": "quux"}]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({"asdf": [{"foo": "bar"}, {"bar": "baz"}]},),
                ({"asdf": [{"baz": "qux"}, {"qux": "quux"}]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_12(self):
        with self.create_table("a JSON"):
            data = [
                ({"fdsa": [["foo", "bar"], ["bar", "baz"]]},),
                ({"fdsa": [["baz", "qux"], ["qux", "quux"]]},),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)

            query = "SELECT * FROM test"
            expected_result = [
                ({"fdsa": [["foo", "bar"], ["bar", "baz"]]},),
                ({"fdsa": [["baz", "qux"], ["qux", "quux"]]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_12_fromcli(self):
        with self.create_table("a JSON"):
            self.emit_cli(
                'INSERT INTO test (a) VALUES (\'{"fdsa": [["foo", "bar"], ["bar", "baz"]]}\'),\
                     (\'{"fdsa": [["baz", "qux"], ["qux", "quux"]]}\')'
            )

            query = "SELECT * FROM test"
            expected_result = [
                ({"fdsa": [["foo", "bar"], ["bar", "baz"]]},),
                ({"fdsa": [["baz", "qux"], ["qux", "quux"]]},),
            ]
            result = self.client.execute(query)
            self.assertEqual(result, expected_result)

    def test_json_shared_paths(self):
        # Paths that overflow max_dynamic_paths land in the JSON column's
        # shared data sub-column. Force a merge so the shared data
        # representation is actually persisted.
        spec = "id UInt32, a JSON(max_dynamic_paths=2, max_dynamic_types=2)"
        template = "CREATE TABLE test ({}) ENGINE = MergeTree ORDER BY id"
        with self.create_table(spec, template=template):
            data = []
            for i in range(8):
                data.append({
                    "id": i,
                    "a": {
                        "common": "shared",
                        "k{}".format(i): i,
                        "num": i * 2,
                    },
                })
            self.client.execute(
                "INSERT INTO test (id, a) VALUES", data)
            self.client.execute("OPTIMIZE TABLE test FINAL")
            result = self.client.execute(
                "SELECT a FROM test ORDER BY id")
            expected = [
                ({
                    "common": "shared",
                    "k{}".format(i): i,
                    "num": i * 2,
                },) for i in range(8)
            ]
            self.assertEqual(result, expected)

    def test_json_shared_paths_mixed_types(self):
        # All paths go to shared data because both limits are zero; the
        # values exercise the binary-encoded type tags for Int, Float,
        # String and Bool.
        spec = "id UInt32, a JSON(max_dynamic_paths=0, max_dynamic_types=0)"
        template = "CREATE TABLE test ({}) ENGINE = MergeTree ORDER BY id"
        with self.create_table(spec, template=template):
            data = [
                {"id": 0, "a": {"x": 1, "y": "two", "z": True}},
                {"id": 1, "a": {"x": 2, "y": "three", "z": False}},
                {"id": 2, "a": {"f": 3.5}},
                {"id": 3, "a": {}},
            ]
            self.client.execute(
                "INSERT INTO test (id, a) VALUES", data)
            self.client.execute("OPTIMIZE TABLE test FINAL")
            result = self.client.execute(
                "SELECT a FROM test ORDER BY id")
            self.assertEqual(result, [
                ({"x": 1, "y": "two", "z": True},),
                ({"x": 2, "y": "three", "z": False},),
                ({"f": 3.5},),
                ({},),
            ])

    def test_nullable_json_round_trip(self):
        with self.create_table("a Nullable(JSON)"):
            data = [(None,), ({"x": 1},), (None,), ({"y": "two"},)]
            self.client.execute("INSERT INTO test (a) VALUES", data)
            result = self.client.execute("SELECT * FROM test")
            self.assertEqual(result, data)

    def test_non_nullable_json_coerces_none_to_empty_dict(self):
        with self.create_table("a JSON"):
            self.client.execute(
                "INSERT INTO test (a) VALUES", [(None,), ({"x": 1},)])
            result = self.client.execute("SELECT * FROM test")
            self.assertEqual(result, [({},), ({"x": 1},)])

    def test_json_array_value_types(self):
        # Each homogeneous array maps to a distinct Array spec on the
        # write path (Int64 / Float64 / Bool / String), and round-trips
        # back through the per-path Dynamic reader.
        with self.create_table("a JSON"):
            data = [
                ({"ints": [1, 2, 3]},),
                ({"floats": [1.5, 2.5]},),
                ({"bools": [True, False]},),
                ({"strings": ["a", "b"]},),
            ]
            self.client.execute("INSERT INTO test (a) VALUES", data)
            result = self.client.execute("SELECT * FROM test")
            self.assertEqual(result, data)

    def test_json_array_mixed_numeric_coerced_to_string(self):
        # A heterogeneous array has no common numeric type, so the
        # server widens every element to String.
        with self.create_table("a JSON"):
            self.client.execute(
                "INSERT INTO test (a) VALUES", [({"v": [1, "x", 2.5]},)])
            result = self.client.execute("SELECT * FROM test")
            self.assertEqual(result, [({"v": ["1", "x", "2.5"]},)])

    def test_json_nested_arrays(self):
        # Arrays whose elements are themselves arrays exercise the
        # recursive Array spec inference / decoder.
        with self.create_table("a JSON"):
            data = [({"grid": [[1, 2], [3]]},)]
            self.client.execute("INSERT INTO test (a) VALUES", data)
            result = self.client.execute("SELECT * FROM test")
            self.assertEqual(result, data)

    def test_json_array_of_objects(self):
        # An array of objects becomes a Tuple spec on the write path and
        # a dynamic path on read.
        with self.create_table("a JSON"):
            data = [({"items": [{"p": 1}, {"p": 2}]},)]
            self.client.execute("INSERT INTO test (a) VALUES", data)
            result = self.client.execute("SELECT * FROM test")
            self.assertEqual(result, data)

    def test_json_deeply_nested_object(self):
        with self.create_table("a JSON"):
            data = [({"a": {"b": {"c": {"d": 1}}}},)]
            self.client.execute("INSERT INTO test (a) VALUES", data)
            result = self.client.execute("SELECT * FROM test")
            self.assertEqual(result, data)

    def test_json_string_value_input(self):
        # A row given as a JSON string is parsed by the write path
        # before serialization.
        with self.create_table("a JSON"):
            self.client.execute(
                "INSERT INTO test (a) VALUES", [('{"x": 1, "y": "two"}',)])
            result = self.client.execute("SELECT * FROM test")
            self.assertEqual(result, [({"x": 1, "y": "two"},)])

    def test_json_shared_arrays(self):
        # Arrays forced into the shared-data sub-column exercise the
        # SharedVariant Array / nested-Array decoders, which the typed
        # dynamic-path reader never reaches.
        spec = "id UInt32, a JSON(max_dynamic_paths=0, max_dynamic_types=0)"
        template = "CREATE TABLE test ({}) ENGINE = MergeTree ORDER BY id"
        with self.create_table(spec, template=template):
            data = [
                {"id": 0, "a": {"ints": [1, 2, 3]}},
                {"id": 1, "a": {"nested": [[1, 2], [3]]}},
                {"id": 2, "a": {"strings": ["a", "b"]}},
                {"id": 3, "a": {"floats": [1.5, 2.5]}},
            ]
            self.client.execute("INSERT INTO test (id, a) VALUES", data)
            self.client.execute("OPTIMIZE TABLE test FINAL")
            result = self.client.execute("SELECT a FROM test ORDER BY id")
            self.assertEqual(result, [
                ({"ints": [1, 2, 3]},),
                ({"nested": [[1, 2], [3]]},),
                ({"strings": ["a", "b"]},),
                ({"floats": [1.5, 2.5]},),
            ])

    def test_json_shared_arrays_of_objects(self):
        # An array of objects forced into shared data serialises as
        # Array(JSON); the SharedVariant decoder has to recurse into the
        # nested JSON value (including dotted paths) to reconstruct it.
        spec = "id UInt32, a JSON(max_dynamic_paths=0, max_dynamic_types=0)"
        template = "CREATE TABLE test ({}) ENGINE = MergeTree ORDER BY id"
        with self.create_table(spec, template=template):
            data = [
                {"id": 0, "a": {"items": [{"p": 1}, {"q": "x"}]}},
                {"id": 1, "a": {"items": [{"nested": {"deep": 5}}]}},
            ]
            self.client.execute("INSERT INTO test (id, a) VALUES", data)
            self.client.execute("OPTIMIZE TABLE test FINAL")
            result = self.client.execute("SELECT a FROM test ORDER BY id")
            self.assertEqual(result, [
                ({"items": [{"p": 1}, {"q": "x"}]},),
                ({"items": [{"nested": {"deep": 5}}]},),
            ])
