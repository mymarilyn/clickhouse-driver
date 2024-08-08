from clickhouse_driver.columns.util import get_inner_columns_with_types


def test_get_inner_columns_with_types_empty_spaces():
    assert (
        get_inner_columns_with_types('\n test String\n ') ==
        [('test', 'String')]
    )
