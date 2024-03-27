from unittest import skipIf

try:
    import pandas as pd

    PANDAS_IMPORTED = True
except ImportError:
    PANDAS_IMPORTED = False

#from tests.testcase import BaseTestCase
from tests.numpy.testcase import NumpyBaseTestCase


@skipIf(not PANDAS_IMPORTED, reason="pandas cannot be imported")
class InsertDataFrameTestCase(NumpyBaseTestCase):
    """The test suite on pandas.DataFrame insertions."""

    def test_insert_ndarray_acceptance(self):
        """https://github.com/mymarilyn/clickhouse-driver/issues/356"""

        with self.create_table("status String"):
            df = pd.DataFrame(columns=["status"])
            rv = self.client.insert_dataframe("INSERT INTO test VALUES", df)
            assert rv == 0

    def test_insert_the_frame_with_dict_rows(self):
        """https://github.com/mymarilyn/clickhouse-driver/issues/417"""

        with self.create_table("a Tuple(x Float32, y Float32)"):
            df = pd.DataFrame(
                [
                    {
                        "a": (0.1, 0.1),
                    },
                ]
            )
            self.client.insert_dataframe("INSERT INTO test VALUES", df)

            df = pd.DataFrame(
                [
                    {
                        "a": {"x": 0.2, "y": 0.2},
                    },
                ]
            )
            self.client.insert_dataframe("INSERT INTO test VALUES", df)

            df = pd.DataFrame(
                [
                    {
                        "a": {"x": 0.3, "y": 0.3},
                    },
                ]
            )
            self.client.insert_dataframe(
                "INSERT INTO test VALUES", df, settings={"use_numpy": False}
            )

            df = pd.DataFrame(
                [
                    {
                        "a": (0.4, 0.4),
                    },
                ]
            )
            self.client.execute("INSERT INTO test VALUES", df.values.tolist())

            df = pd.DataFrame(
                [
                    {
                        "a": (0.5, 0.5),
                    },
                ]
            )
            self.client.execute(
                "INSERT INTO test VALUES",
                df.values.tolist(),
                settings={"use_numpy": False},
            )

            query = "SELECT * FROM test"
            result = self.client.execute(query)
            # normalising the result
            result = [
                tuple(map(lambda val: round(val, 1), row[0])) for row in result
            ]

            # to enforce the order and prevent the test from failing
            self.assertEqual(
                sorted(result),
                [(0.1, 0.1), (0.2, 0.2), (0.3, 0.3), (0.4, 0.4), (0.5, 0.5)],
            )
