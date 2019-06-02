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

Each setting can be overridden in an `execute` statement:

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
Additional packages should be install in order by enable compression suport, see :ref:`installation-pypi`.
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

* progress: processed rows, bytes and total rows

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
        2018-12-14 10:24:53,873 INFO     clickhouse_driver.log: {b328ad33-60e8-4012-b4cc-97f44a7b28f2} [ 25 ] <Debug> executeQuery: (from 127.0.0.1:57762) SELECT 1
        2018-12-14 10:24:53,874 INFO     clickhouse_driver.log: {b328ad33-60e8-4012-b4cc-97f44a7b28f2} [ 25 ] <Debug> executeQuery: Query pipeline:
        Expression
         Expression
          One

        2018-12-14 10:24:53,875 INFO     clickhouse_driver.log: {b328ad33-60e8-4012-b4cc-97f44a7b28f2} [ 25 ] <Information> executeQuery: Read 1 rows, 1.00 B in 0.004 sec., 262 rows/sec., 262.32 B/sec.
        2018-12-14 10:24:53,875 INFO     clickhouse_driver.log: {b328ad33-60e8-4012-b4cc-97f44a7b28f2} [ 25 ] <Debug> MemoryTracker: Peak memory usage (for query): 40.23 KiB.
        [(1,)]
