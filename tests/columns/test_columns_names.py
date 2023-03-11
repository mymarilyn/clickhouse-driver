from tests.testcase import BaseTestCase


class ColumnsNamesTestCase(BaseTestCase):

    def test_columns_names_replace_nonwords(self):
        columns = (
            'regular Int64, '
            'CamelCase Int64, '
            'With_Underscores Int64, '
            'Any%different.Column? Int64'
        )

        expected_columns = [
            'regular', 'CamelCase', 'With_Underscores', 'Any%different.Column?'
        ]

        with self.create_table(columns):
            df = self.client.query_dataframe(
                'SELECT * FROM test', replace_nonwords=False
            )
            self.assertTrue(expected_columns.equals(list(df.columns)))
