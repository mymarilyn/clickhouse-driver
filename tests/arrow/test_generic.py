try:
    import pyarrow as pa
except ImportError:
    pa = None

from clickhouse_driver import errors
from tests.testcase import BaseTestCase
from tests.arrow.testcase import ArrowBaseTestCase


class QueryArrowTestCase(ArrowBaseTestCase):
    def test_simple(self):
        table = self.client.query_arrow(
            'SELECT number FROM system.numbers LIMIT 100'
        )

        self.assertIsInstance(table, pa.Table)
        self.assertEqual(table.num_rows, 100)
        self.assertEqual(table.column_names, ['number'])
        self.assertEqual(table.schema.field('number').type, pa.uint64())
        self.assertEqual(table.column('number').to_pylist(), list(range(100)))

    def test_column_names_preserved(self):
        table = self.client.query_arrow('SELECT 1 AS "my col"')

        self.assertEqual(table.column_names, ['my col'])

    def test_multiple_columns(self):
        table = self.client.query_arrow(
            'SELECT '
            'CAST(number AS Int32) AS x, '
            'toString(number) AS y '
            'FROM system.numbers LIMIT 3'
        )

        self.assertEqual(table.column_names, ['x', 'y'])
        self.assertEqual(table.column('x').to_pylist(), [0, 1, 2])
        self.assertEqual(table.column('y').to_pylist(), ['0', '1', '2'])

    def test_empty_result_keeps_schema(self):
        table = self.client.query_arrow(
            'SELECT CAST(number AS Int32) AS x, toString(number) AS y '
            'FROM system.numbers LIMIT 0'
        )

        self.assertEqual(table.num_rows, 0)
        self.assertEqual(table.schema.field('x').type, pa.int32())
        self.assertEqual(table.schema.field('y').type, pa.string())

    def test_params_substitution(self):
        table = self.client.query_arrow(
            'SELECT CAST(%(x)s AS Int64) AS x', params={'x': 42}
        )

        self.assertEqual(table.column('x').to_pylist(), [42])

    def test_settings(self):
        table = self.client.query_arrow(
            'SELECT number FROM system.numbers LIMIT 1000',
            settings={'max_block_size': 100}
        )

        self.assertEqual(table.num_rows, 1000)

    def test_external_tables(self):
        table = self.client.query_arrow(
            'SELECT * FROM ext ORDER BY x',
            external_tables=[{
                'name': 'ext',
                'structure': [('x', 'Int32')],
                'data': [{'x': 1}, {'x': 2}]
            }]
        )

        self.assertEqual(table.column('x').to_pylist(), [1, 2])

    def test_field_metadata(self):
        table = self.client.query_arrow(
            'SELECT CAST(1 AS Nullable(Int32)) AS x, '
            'toLowCardinality(toString(1)) AS lc'
        )

        self.assertEqual(
            table.schema.field('x').metadata,
            {b'clickhouse_type': b'Nullable(Int32)'}
        )
        self.assertEqual(
            table.schema.field('lc').metadata,
            {b'clickhouse_type': b'LowCardinality(String)'}
        )

    def test_field_metadata_opt_out(self):
        table = self.client.query_arrow(
            'SELECT 1 AS x', field_metadata=False
        )

        self.assertIsNone(table.schema.field('x').metadata)

    def test_several_blocks_concatenated(self):
        table = self.client.query_arrow(
            'SELECT number FROM system.numbers LIMIT 1000',
            settings={'max_block_size': 100}
        )

        self.assertEqual(table.column('number').to_pylist(),
                         list(range(1000)))


class QueryArrowStreamTestCase(ArrowBaseTestCase):
    def test_returns_record_batch_reader(self):
        reader = self.client.query_arrow_stream(
            'SELECT number FROM system.numbers LIMIT 10'
        )

        self.assertIsInstance(reader, pa.RecordBatchReader)
        reader.close()

    def test_schema_available_before_consumption(self):
        reader = self.client.query_arrow_stream(
            'SELECT CAST(number AS Int32) AS x '
            'FROM system.numbers LIMIT 10'
        )

        self.assertEqual(reader.schema.field('x').type, pa.int32())
        reader.close()

    def test_stream_field_metadata(self):
        reader = self.client.query_arrow_stream(
            'SELECT number FROM system.numbers LIMIT 10'
        )

        self.assertEqual(
            reader.schema.field('number').metadata,
            {b'clickhouse_type': b'UInt64'}
        )
        reader.read_all()

    def test_multiple_batches(self):
        reader = self.client.query_arrow_stream(
            'SELECT number FROM system.numbers LIMIT 1000',
            settings={'max_block_size': 100}
        )

        batches = list(reader)
        self.assertGreater(len(batches), 1)
        for batch in batches:
            self.assertIsInstance(batch, pa.RecordBatch)

        total_rows = sum(batch.num_rows for batch in batches)
        self.assertEqual(total_rows, 1000)

    def test_read_all_equals_query_arrow(self):
        query = ('SELECT number, toString(number) AS s '
                 'FROM system.numbers LIMIT 1000')
        settings = {'max_block_size': 100}

        reader = self.client.query_arrow_stream(query, settings=settings)
        streamed = reader.read_all()

        table = self.client.query_arrow(query, settings=settings)
        self.assertTrue(streamed.equals(table))

    def test_empty_result(self):
        reader = self.client.query_arrow_stream(
            'SELECT CAST(number AS Int32) AS x FROM system.numbers LIMIT 0'
        )

        table = reader.read_all()
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(table.schema.field('x').type, pa.int32())

    def test_client_usable_after_stream_consumed(self):
        reader = self.client.query_arrow_stream(
            'SELECT number FROM system.numbers LIMIT 10'
        )
        reader.read_all()

        rv = self.client.execute('SELECT 1')
        self.assertEqual(rv, [(1, )])

    def test_client_usable_after_close_before_consuming(self):
        reader = self.client.query_arrow_stream(
            'SELECT number FROM system.numbers LIMIT 100000',
            settings={'max_block_size': 100}
        )
        reader.close()

        rv = self.client.execute('SELECT 1')
        self.assertEqual(rv, [(1, )])

    def test_client_usable_after_close_after_one_batch(self):
        reader = self.client.query_arrow_stream(
            'SELECT number FROM system.numbers LIMIT 100000',
            settings={'max_block_size': 100}
        )
        batch = reader.read_next_batch()
        self.assertEqual(batch.num_rows, 100)
        reader.close()

        rv = self.client.execute('SELECT 1')
        self.assertEqual(rv, [(1, )])

    def test_arrow_query_after_close_after_one_batch(self):
        reader = self.client.query_arrow_stream(
            'SELECT number FROM system.numbers LIMIT 100000',
            settings={'max_block_size': 100}
        )
        reader.read_next_batch()
        reader.close()

        table = self.client.query_arrow(
            'SELECT number FROM system.numbers LIMIT 10'
        )
        self.assertEqual(table.column('number').to_pylist(), list(range(10)))

    def test_abandoned_reader_raises_after_next_query(self):
        reader = self.client.query_arrow_stream(
            'SELECT number FROM system.numbers LIMIT 100000',
            settings={'max_block_size': 100}
        )
        reader.read_next_batch()

        self.client.execute('SELECT 1')

        # The streamed query was cancelled by the query above: reading
        # further must fail loudly instead of consuming its packets.
        with self.assertRaises(errors.PartiallyConsumedQueryError):
            reader.read_all()


class ArrowNumpyPathTestCase(ArrowBaseTestCase):
    """
    query_arrow must return identical results with and without
    use_numpy fast path.
    """

    def setUp(self):
        super(ArrowNumpyPathTestCase, self).setUp()

        try:
            import numpy  # noqa: F401
            import pandas  # noqa: F401
        except ImportError:
            self.skipTest('NumPy package is not installed')

    def test_results_equal_with_use_numpy(self):
        query = (
            'SELECT '
            'CAST(number AS Int64) AS x, '
            'toString(number) AS y, '
            'CAST(number AS Float64) / 3 AS z, '
            "toDateTime(number, 'UTC') AS dt, "
            "toDateTime64(number + 0.123, 3, 'UTC') AS dt64, "
            'toLowCardinality(toString(number % 3)) AS lc '
            'FROM system.numbers LIMIT 1000'
        )

        table = self.client.query_arrow(query)

        with self.created_client(settings={'use_numpy': True}) as client:
            numpy_table = client.query_arrow(query)

        self.assertTrue(table.equals(numpy_table))

    def test_long_strings_equal_with_use_numpy(self):
        # Strings longer than the reader buffer exercise the
        # buffer-spanning path of the Arrow string reader.
        query = (
            "SELECT concat('привет', repeat('x', 1000000), "
            "repeat('y', 1000000), toString(number)) AS s "
            'FROM system.numbers LIMIT 3'
        )

        table = self.client.query_arrow(query)

        with self.created_client(settings={'use_numpy': True}) as client:
            numpy_table = client.query_arrow(query)

        self.assertTrue(table.equals(numpy_table))

    def test_nullable_results_equal_with_use_numpy(self):
        query = (
            'SELECT '
            'CAST(if(number % 2 = 0, NULL, number) AS Nullable(Int64)) '
            'AS x, '
            'CAST(if(number % 3 = 0, NULL, number / 7) '
            'AS Nullable(Float64)) AS f, '
            'CAST(if(number % 4 = 0, NULL, toString(number)) '
            'AS Nullable(String)) AS s, '
            'CAST(if(number % 6 = 0, NULL, toString(number % 10)) '
            'AS Nullable(FixedString(2))) AS fs, '
            "CAST(if(number % 5 = 0, NULL, toDateTime(number, 'UTC')) "
            "AS Nullable(DateTime('UTC'))) AS dt "
            'FROM system.numbers LIMIT 10'
        )

        table = self.client.query_arrow(query)

        with self.created_client(settings={'use_numpy': True}) as client:
            numpy_table = client.query_arrow(query)

        self.assertTrue(table.equals(numpy_table))
        self.assertEqual(table.column('x').null_count, 5)


class NoPyArrowTestCase(BaseTestCase):
    def setUp(self):
        super(NoPyArrowTestCase, self).setUp()

        try:
            import pyarrow  # noqa: F401
        except ImportError:
            pass

        else:
            self.skipTest('PyArrow extras are installed')

    def test_query_arrow(self):
        with self.assertRaises(RuntimeError) as e:
            self.client.query_arrow('SELECT 1 AS x')

        self.assertEqual(
            'Extras for PyArrow must be installed', str(e.exception)
        )

    def test_query_arrow_stream(self):
        with self.assertRaises(RuntimeError) as e:
            self.client.query_arrow_stream('SELECT 1 AS x')

        self.assertEqual(
            'Extras for PyArrow must be installed', str(e.exception)
        )
