try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.testcase import NumpyBaseTestCase


class StringTestCase(NumpyBaseTestCase):
    def test_string(self):
        query = "SELECT arrayJoin(splitByChar(',', 'a,b,c')) AS x"
        rv = self.client.execute(query, columnar=True)

        self.assertArraysEqual(rv[0], np.array(['a', 'b', 'c']))
        self.assertIsInstance(rv[0][0], (object, ))


class ByteStringTestCase(NumpyBaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True, 'use_numpy': True}}

    def test_string(self):
        query = "SELECT arrayJoin(splitByChar(',', 'a,b,c')) AS x"
        rv = self.client.execute(query, columnar=True)

        self.assertArraysEqual(rv[0], np.array([b'a', b'b', b'c']))
        self.assertIsInstance(rv[0][0], (object, ))


class FixedStringTestCase(NumpyBaseTestCase):
    def test_string(self):
        query = (
            "SELECT CAST(arrayJoin(splitByChar(',', 'a,b,c')) "
            "AS FixedString(2)) AS x"
        )
        rv = self.client.execute(query, columnar=True)

        self.assertArraysEqual(rv[0], np.array(['a', 'b', 'c']))
        self.assertIsInstance(rv[0][0], (object, ))


class ByteFixedStringTestCase(NumpyBaseTestCase):
    client_kwargs = {'settings': {'strings_as_bytes': True, 'use_numpy': True}}

    def test_string(self):
        query = (
            "SELECT CAST(arrayJoin(splitByChar(',', 'a,b,c')) "
            "AS FixedString(3)) AS x"
        )
        rv = self.client.execute(query, columnar=True)

        self.assertArraysEqual(rv[0], np.array([b'a', b'b', b'c']))
        self.assertIsInstance(rv[0][0], (object, ))
