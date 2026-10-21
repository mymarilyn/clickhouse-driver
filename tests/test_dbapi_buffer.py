from unittest.mock import Mock
from weakref import ref

import pytest

from clickhouse_driver.dbapi.cursor import Cursor
from clickhouse_driver.dbapi.extras import DictCursor, NamedTupleCursor


@pytest.fixture(params=[Cursor, DictCursor, NamedTupleCursor])
def cursor(request):
    client = Mock()
    columns = [('number', 'UInt64')]
    client.execute.side_effect = lambda *args, **kwargs: (
        [(i,) for i in range(8)], columns
    )
    return request.param(client, Mock())


def test_mixed_fetches_preserve_order_and_factories(cursor):
    def expected(number):
        if isinstance(cursor, DictCursor):
            return {'number': number}
        if isinstance(cursor, NamedTupleCursor):
            return cursor._make_nt(('number',))(number)
        return (number,)

    cursor.execute('SELECT number FROM numbers(8)')
    cursor.arraysize = 2

    assert cursor.fetchmany(0) == []
    assert cursor.fetchone() == expected(0)
    assert cursor.fetchmany() == [expected(1), expected(2)]
    assert cursor.fetchmany(1) == [expected(3)]
    rows = iter(cursor)
    assert next(rows) == expected(4)
    assert cursor.fetchall() == [expected(i) for i in range(5, 8)]
    assert list(rows) == []
    assert cursor.fetchone() is None
    assert cursor.fetchmany() == []
    assert cursor.fetchall() == []
    assert cursor.rowcount == 8


@pytest.mark.parametrize('fetch', ['fetchone', 'fetchmany', 'iteration'])
def test_incremental_fetches_avoid_shifting_or_copying_remaining_rows(fetch):
    class BufferedRows(list):
        def pop(self, position=-1):
            assert position == -1, 'must not shift the remaining rows'
            return super().pop(position)

        def __getitem__(self, key):
            if (isinstance(key, slice) and key.start is not None
                    and key.start > 0 and key.stop is None):
                raise AssertionError('must not copy the remaining rows')
            return super().__getitem__(key)

    client = Mock()
    client.execute.side_effect = lambda *args, **kwargs: (
        BufferedRows((i,) for i in range(129)), [('number', 'UInt64')]
    )
    cursor = Cursor(client, Mock())
    cursor.execute('SELECT number FROM numbers(129)')

    if fetch == 'fetchone':
        actual = [cursor.fetchone() for _ in range(129)]
    elif fetch == 'fetchmany':
        actual = []
        while True:
            batch = cursor.fetchmany(10)
            if not batch:
                break
            actual.extend(batch)
    else:
        actual = list(cursor)

    assert actual == [(i,) for i in range(129)]
    assert cursor.fetchone() is None


@pytest.mark.parametrize('fetch', ['fetchone', 'fetchmany'])
def test_incremental_fetches_release_consumed_payloads(fetch):
    refs = []

    class TrackedString(str):
        __slots__ = ('__weakref__',)

    def response(*args, **kwargs):
        values = [TrackedString(str(i)) for i in range(3)]
        refs.extend(ref(value) for value in values)
        return [(value,) for value in values], [('value', 'String')]

    client = Mock()
    client.execute.side_effect = response
    cursor = Cursor(client, Mock())
    cursor.execute('SELECT value')

    if fetch == 'fetchone':
        consumed = cursor.fetchone()
    else:
        consumed = cursor.fetchmany(1)
    assert refs[0]() is not None
    del consumed

    assert refs[0]() is None
    assert all(r() is not None for r in refs[1:])
    assert cursor.fetchall() == [('1',), ('2',)]


def test_unconsumed_fetchall_transfers_the_existing_list():
    client = Mock()
    client.execute.side_effect = lambda *args, **kwargs: (
        [(i,) for i in range(3)], [('number', 'UInt64')]
    )
    cursor = Cursor(client, Mock())
    cursor.execute('SELECT number FROM numbers(3)')
    buffered = cursor._rows

    assert cursor.fetchall() is buffered
    assert buffered == [(0,), (1,), (2,)]
    assert cursor.fetchall() == []


def test_new_query_resets_incremental_fetch_order(cursor):
    cursor.execute('SELECT number FROM numbers(8)')
    first = cursor.fetchone()

    cursor.execute('SELECT number FROM numbers(8)')

    assert cursor.fetchone() == first
    assert len(cursor.fetchall()) == 7


def test_empty_results_after_partial_consumption(cursor):
    cursor.execute('SELECT number FROM numbers(8)')
    cursor.fetchone()
    cursor._client.execute.return_value = ([], [('number', 'UInt64')])
    cursor._client.execute.side_effect = None
    cursor.execute('SELECT number FROM numbers(0)')

    assert cursor.rowcount == 0
    assert cursor.fetchone() is None
    assert cursor.fetchmany() == []
    assert cursor.fetchall() == []
