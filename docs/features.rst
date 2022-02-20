.. _features:

Features
========

- Compression support:

  * `LZ4/LZ4HC <http://www.lz4.org/>`_
  * `ZSTD <https://facebook.github.io/zstd/>`_

- TLS support (since server version 1.1.54304).


.. _external-tables:

External data for query processing
----------------------------------

You can pass `external data <https://clickhouse.yandex/docs/en/single/index.html#external-data-for-query-processing>`_
alongside with query:

    .. code-block:: python

        >>> tables = [{
        ...     'name': 'ext',
        ...     'structure': [('x', 'Int32'), ('y', 'Array(Int32)')],
        ...     'data': [
        ...         {'x': 100, 'y': [2, 4, 6, 8]},
        ...         {'x': 500, 'y': [1, 3, 5, 7]},
        ...     ]
        ... }]
        >>> client.execute(
        ...     'SELECT sum(x) FROM ext', external_tables=tables
        ... )
        [(600,)]


Settings
--------

There are a lot of ClickHouse server `settings <https://clickhouse.yandex/docs/en/single/index.html#server-settings>`_.
Settings can be specified during Client initialization:

    .. code-block:: python

        # Set max number threads for all queries execution.
        >>> settings = {'max_threads': 2}
        >>> client = Client('localhost', settings=settings)

Each setting can be overridden in an ``execute``, ``execute_with_progress`` and
``execute_iter`` statement:

    .. code-block:: python

        # Set lower priority to query and limit max number threads
        # to execute the request.
        >>> settings = {'max_threads': 2, 'priority': 10}
        >>> client.execute('SHOW TABLES', settings=settings)
        [('first_table',)]


Compression
-----------

Native protocol supports two types of compression: `LZ4 <http://www.lz4.org/>`_ and
`ZSTD <https://facebook.github.io/zstd/>`_. When compression is enabled compressed data
should be hashed using `CityHash algorithm <https://github.com/google/cityhash>`_.
Additional packages should be installed in order by enable compression support, see :ref:`installation-pypi`.
Enabled client-side compression can save network traffic.

Client with compression support can be constructed as follows:

    .. code-block:: python

        >>> from clickhouse_driver import Client
        >>> client_with_lz4 = Client('localhost', compression=True)
        >>> client_with_lz4 = Client('localhost', compression='lz4')
        >>> client_with_zstd = Client('localhost', compression='zstd')


.. _compression-cityhash-notes:

CityHash algorithm notes
~~~~~~~~~~~~~~~~~~~~~~~~

Unfortunately ClickHouse server comes with built-in old version of CityHash algorithm (1.0.2).
That's why we can't use original `CityHash <https://pypi.org/project/cityhash>`_ package.
An older version is published separately at `PyPI <https://pypi.org/project/clickhouse-cityhash>`_.


Secure connection
-----------------

    .. code-block:: python

        >>> from clickhouse_driver import Client
        >>>
        >>> client = Client('localhost', secure=True)
        >>> # Using self-signed certificate.
        ... self_signed_client = Client(
        ...     'localhost', secure=True,
        ...     ca_certs='/etc/clickhouse-server/server.crt'
        ... )
        >>> # Disable verification.
        ... no_verifyed_client = Client(
        ...     'localhost', secure=True, verify=False
        ... )
        >>>
        >>> # Example of secured client with Let's Encrypt certificate.
        ... import certifi
        >>>
        >>> client = Client(
        ...     'remote-host', secure=True, ca_certs=certifi.where()
        ... )


Specifying query id
-------------------

You can manually set query identificator for each query. UUID for example:

    .. code-block:: python

        >>> from uuid import uuid4
        >>>
        >>> query_id = str(uuid4())
        >>> print(query_id)
        bbd7dea3-eb63-4a21-b727-f55b420a7223
        >>> client.execute(
        ...     'SELECT * FROM system.processes', query_id=query_id
        ... )
        [(1, 'default', 'bbd7dea3-eb63-4a21-b727-f55b420a7223', '127.0.0.1', 57664, 'default', 'bbd7dea3-eb63-4a21-b727-f55b420a7223', '127.0.0.1', 57664, 1, 'klebedev', 'klebedev-ThinkPad-T460', 'ClickHouse python-driver', 18, 10, 3, 54406, 0, '', '', 0.004916541, 0, 0, 0, 0, 0, 0, 0, 0, 'SELECT * FROM system.processes', (25,), ('Query', 'SelectQuery', 'NetworkReceiveElapsedMicroseconds', 'ContextLock', 'RWLockAcquiredReadLocks'), (1, 1, 54, 9, 1), ('use_uncompressed_cache', 'load_balancing', 'max_memory_usage'), ('0', 'random', '10000000000'))]

You can cancel query with specific id by sending another query with the same
query id if option `replace_running_query
<https://clickhouse.yandex/docs/en/single/#replace-running-query>`_ is set to 1.

Query results are fetched by the same instance of Client that emitted query.

Retrieving results in columnar form
-----------------------------------

Columnar form sometimes can be more useful.

    .. code-block:: python

        >>> client.execute('SELECT arrayJoin(range(3))', columnar=True)
        [(0, 1, 2)]


Data types checking on INSERT
-----------------------------

Data types check is disabled for performance on ``INSERT`` queries.
You can turn it on by `types_check` option:

    .. code-block:: python

        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES', [('abc', )],
        ...     types_check=True
        ... )
        1


Query execution statistics
--------------------------

Client stores statistics about last query execution. It can be obtained by
accessing `last_query` attribute.
Statistics is sent from ClickHouse server and calculated on client side.
`last_query` contains info about:

* profile: rows before limit

    .. code-block:: python

        >>> client.execute('SELECT arrayJoin(range(100)) LIMIT 3')
        [(0,), (1,), (2,)]
        >>> client.last_query.profile_info.rows_before_limit
        100

* progress:
    - processed rows;
    - processed bytes;
    - total rows;
    - written rows (*new in version 0.1.3*);
    - written bytes (*new in version 0.1.3*);

    .. code-block:: python

        >>> client.execute('SELECT max(number) FROM numbers(10)')
        [(9,)]
        >>> client.last_query.progress.rows
        10
        >>> client.last_query.progress.bytes
        80
        >>> client.last_query.progress.total_rows
        10

* elapsed time:

    .. code-block:: python

        >>> client.execute('SELECT sleep(1)')
        [(0,)]
        >>> client.last_query.elapsed
        1.0060372352600098


Receiving server logs
---------------------

Query logs can be received from server by using `send_logs_level` setting:

    .. code-block:: python

        >>> from logging.config import dictConfig
        >>> # Simple logging configuration.
        ... dictConfig({
        ...     'version': 1,
        ...     'disable_existing_loggers': False,
        ...     'formatters': {
        ...         'standard': {
        ...             'format': '%(asctime)s %(levelname)-8s %(name)s: %(message)s'
        ...         },
        ...     },
        ...     'handlers': {
        ...         'default': {
        ...             'level': 'INFO',
        ...             'formatter': 'standard',
        ...             'class': 'logging.StreamHandler',
        ...         },
        ...     },
        ...     'loggers': {
        ...         '': {
        ...             'handlers': ['default'],
        ...             'level': 'INFO',
        ...             'propagate': True
        ...         },
        ...     }
        ... })
        >>>
        >>> settings = {'send_logs_level': 'debug'}
        >>> client.execute('SELECT 1', settings=settings)
        2018-12-14 10:24:53,873 INFO     clickhouse_driver.log: [ klebedev-ThinkPad-T460 ] [ 25 ] {b328ad33-60e8-4012-b4cc-97f44a7b28f2} <Debug> executeQuery: (from 127.0.0.1:57762) SELECT 1
        2018-12-14 10:24:53,874 INFO     clickhouse_driver.log: [ klebedev-ThinkPad-T460 ] [ 25 ] {b328ad33-60e8-4012-b4cc-97f44a7b28f2} <Debug> executeQuery: Query pipeline:
        Expression
         Expression
          One

        2018-12-14 10:24:53,875 INFO     clickhouse_driver.log: [ klebedev-ThinkPad-T460 ] [ 25 ] {b328ad33-60e8-4012-b4cc-97f44a7b28f2} <Information> executeQuery: Read 1 rows, 1.00 B in 0.004 sec., 262 rows/sec., 262.32 B/sec.
        2018-12-14 10:24:53,875 INFO     clickhouse_driver.log: [ klebedev-ThinkPad-T460 ] [ 25 ] {b328ad33-60e8-4012-b4cc-97f44a7b28f2} <Debug> MemoryTracker: Peak memory usage (for query): 40.23 KiB.
        [(1,)]


Multiple hosts
--------------

*New in version 0.1.3.*

Additional connection points can be defined by using `alt_hosts`.
If main connection point is unavailable driver will use next one from `alt_hosts`.

This option is good for ClickHouse cluster with multiple replicas.

    .. code-block:: python

        >>> from clickhouse_driver import Client
        >>> client = Client('host1', alt_hosts='host2:1234,host3,host4:5678')

In example above on every *new* connection driver will use following sequence
of hosts if previous host is unavailable:

* host1:9000;
* host2:1234;
* host3:9000;
* host4:5678.

All queries within established connection will be sent to the same host.


Python DB API 2.0
-----------------

*New in version 0.1.3.*

This driver is also implements `DB API 2.0 specification
<https://www.python.org/dev/peps/pep-0249/>`_. It can be useful for various
integrations.

Threads may share the module and connections.

Parameters are expected in Python extended format codes, e.g.
`...WHERE name=%(name)s`.

    .. code-block:: python

        >>> from clickhouse_driver import connect
        >>> conn = connect('clickhouse://localhost')
        >>> cursor = conn.cursor()
        >>>
        >>> cursor.execute('SHOW TABLES')
        >>> cursor.fetchall()
        [('test',)]
        >>> cursor.execute('DROP TABLE IF EXISTS test')
        >>> cursor.fetchall()
        []
        >>> cursor.execute('CREATE TABLE test (x Int32) ENGINE = Memory')
        >>> cursor.fetchall()
        []
        >>> cursor.executemany(
        ...     'INSERT INTO test (x) VALUES',
        ...     [{'x': 100}]
        ... )
        >>> cursor.rowcount
        1
        >>> cursor.executemany('INSERT INTO test (x) VALUES', [[200]])
        >>> cursor.rowcount
        1
        >>> cursor.execute(
        ...     'INSERT INTO test (x) '
        ...     'SELECT * FROM system.numbers LIMIT %(limit)s',
        ...     {'limit': 3}
        ... )
        >>> cursor.rowcount
        0
        >>> cursor.execute('SELECT sum(x) FROM test')
        >>> cursor.fetchall()
        [(303,)]

ClickHouse native protocol is synchronous: when you emit query in connection
you must read whole server response before sending next query through this
connection. To make DB API thread-safe each cursor should use it's own
connection to the server. In  Under the hood :ref:`dbapi-cursor` is wrapper
around pure :ref:`api-client`.

:ref:`dbapi-connection` class is just wrapper for handling multiple cursors
(clients) and do not initiate actual connections to the ClickHouse server.

There are some non-standard ClickHouse-related :ref:`Cursor methods
<dbapi-cursor>` for: external data, settings, etc.

For automatic disposal Connection and Cursor instances can be used as context
managers:

    .. code-block:: python

        >>> with connect('clickhouse://localhost') as conn:
        >>>     with conn.cursor() as cursor:
        >>>        cursor.execute('SHOW TABLES')
        >>>        print(cursor.fetchall())


You can use ``cursor_factory`` argument to get results as dicts or named tuples
(since version 0.2.4):

    .. code-block:: python

        >>> from clickhouse_driver.dbapi.extras import DictCursor
        >>> with connect('clickhouse://localhost') as conn:
        >>>     with conn.cursor(cursor_factory=DictCursor) as cursor:
        >>>        cursor.execute('SELECT * FROM system.tables')
        >>>        print(cursor.fetchall())

    .. code-block:: python

        >>> from clickhouse_driver.dbapi.extras import NamedTupleCursor
        >>> with connect('clickhouse://localhost') as conn:
        >>>     with conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
        >>>        cursor.execute('SELECT * FROM system.tables')
        >>>        print(cursor.fetchall())


NumPy/Pandas support
--------------------

*New in version 0.1.6.*

Starting from version 0.1.6 package can SELECT and INSERT columns as NumPy
arrays. Additional packages are required for :ref:`installation-numpy-support`.

    .. code-block:: python

        >>> client = Client('localhost', settings={'use_numpy': True}):
        >>> client.execute(
        ...     'SELECT * FROM system.numbers LIMIT 10000',
        ...     columnar=True
        ... )
        [array([   0,    1,    2, ..., 9997, 9998, 9999], dtype=uint64)]


Supported types:

  * Float32/64
  * [U]Int8/16/32/64
  * Date/DateTime('timezone')/DateTime64('timezone')
  * String/FixedString(N)
  * LowCardinality(T)
  * Nullable(T)

Direct loading into NumPy arrays increases performance and lowers memory
requirements on large amounts of rows.

Direct loading into pandas DataFrame is also supported by using
`query_dataframe`:

    .. code-block:: python

        >>> client = Client('localhost', settings={'use_numpy': True})
        >>> client.query_dataframe('
        ...     'SELECT number AS x, (number + 100) AS y '
        ...     'FROM system.numbers LIMIT 10000'
        ... )
                 x      y
        0        0    100
        1        1    101
        2        2    102
        3        3    103
        4        4    104
        ...    ...    ...
        9995  9995  10095
        9996  9996  10096
        9997  9997  10097
        9998  9998  10098
        9999  9999  10099

        [10000 rows x 2 columns]

Writing pandas DataFrame is also supported with `insert_dataframe`:

    .. code-block:: python

        >>> client = Client('localhost', settings={'use_numpy': True})
        >>> client.execute(
        ...    'CREATE TABLE test (x Int64, y Int64) Engine = Memory'
        ... )
        >>> []
        >>> df = client.query_dataframe(
        ...     'SELECT number AS x, (number + 100) AS y '
        ...     'FROM system.numbers LIMIT 10000'
        ... )
        >>> client.insert_dataframe('INSERT INTO test VALUES', df)
        >>> 10000

Starting from version 0.2.2 nullable columns are also supported. Keep in mind
that nullable columns have ``object`` dtype. For convenience ``np.nan`` and
``None`` is supported as ``NULL`` values for inserting. But only ``None`` is
returned after selecting for ``NULL`` values.

    .. code-block:: python

        >>> client = Client('localhost', settings={'use_numpy': True})
        >>> client.execute(
        ...    'CREATE TABLE test ('
        ...    'a Nullable(Int64),
        ...    'b Nullable(Float64),
        ...    'c Nullable(String)'
        ...    ') Engine = Memory'
        ... )
        >>> []
        >>> df = pd.DataFrame({
        ...     'a': [1, None, None],
        ...     'b': [1.0, None, np.nan],
        ...     'c': ['a', None, np.nan],
        ... }, dtype=object)
        >>> client.insert_dataframe('INSERT INTO test VALUES', df)
        3
        >>> client.query_dataframe('SELECT * FROM test')
              a     b     c
        0     1     1     a
        1  None  None  None
        2  None   NaN  None

It's important to specify `dtype` during dataframe creation:

    .. code-block:: python

        >>> bad_df = pd.DataFrame({
        ...     'a': [1, None, None],
        ...     'b': [1.0, None, np.nan],
        ...     'c': ['a', None, np.nan],
        ... })
        >>> bad_df
             a    b     c
        0  1.0  1.0     a
        1  NaN  NaN  None
        2  NaN  NaN   NaN
        >>> good_df = pd.DataFrame({
        ...     'a': [1, None, None],
        ...     'b': [1.0, None, np.nan],
        ...     'c': ['a', None, np.nan],
        ... }, dtype=object)
        >>> good_df
              a     b     c
        0     1     1     a
        1  None  None  None
        2  None   NaN   NaN

As you can see float column ``b`` in ``bad_df`` has two ``NaN`` values.
But ``NaN`` and ``None`` is not the same for float point numbers.
``NaN`` is ``float('nan')`` where ``None`` is representing ``NULL``.

Automatic disposal
------------------

*New in version 0.2.2.*

Each Client instance can be used as a context manager:

    .. code-block:: python

        >>> with Client('localhost') as client:
        >>>     client.execute('SELECT 1')


Upon exit, any established connection to the ClickHouse server will be closed
automatically.
