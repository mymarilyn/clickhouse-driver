.. _quickstart:

Quickstart
==========

This page gives a good introduction to clickhouse-driver.
It assumes you already have clickhouse-driver installed.
If you do not, head over to the :ref:`installation` section.

A minimal working example looks like this:

    .. code-block:: python

        >>> from clickhouse_driver import Client
        >>>
        >>> client = Client(host='localhost')
        >>>
        >>> client.execute('SHOW DATABASES')
        [('default',)]


This code will show all tables from ``'default'`` database.

There are two conceptual types of queries:

- Read only queries: SELECT, SHOW, etc.
- Read and write queries: INSERT.


Every query should be executed by calling one of the client's execute
methods: `execute`, `execute_with_progress`, `execute_iter method`.

- SELECT queries can use `execute`, `execute_with_progress`, `execute_iter`
  methods.
- INSERT queries can use only `execute` method.

Selecting data
--------------

Simple select query looks like:

    .. code-block:: python

        >>> client.execute('SELECT * FROM system.numbers LIMIT 5')
        [(0,), (1,), (2,), (3,), (4,)]


Of course queries can and should be parameterized to avoid SQL injections:

    .. code-block:: python

        >>> from datetime import date
        >>> client.execute(
        ...     'SELECT %(date)s, %(a)s + %(b)s',
        ...     {'date': date.today(), 'a': 1, 'b': 2}
        ... )
        [('2018-10-21', 3)]

Percent symbols in inlined constants should be doubled if you mix constants
with ``%`` symbol and ``%(myvar)s`` parameters.

    .. code-block:: python

        >>> client.execute(
        ...     "SELECT 'test' like '%%es%%', %(myvar)s",
        ...     {'myvar': 1}
        ... )

NOTE: formatting queries using Python's f-strings or concatenation can lead to SQL injections.
Use ``%(myvar)s`` parameters instead.

Customisation ``SELECT`` output with ``FORMAT`` clause is not supported.

.. _execute-with-progress:

Selecting data with progress statistics
---------------------------------------

You can get query progress statistics by using `execute_with_progress`. It can be useful for cancelling long queries.

    .. code-block:: python

        >>> from datetime import datetime
        >>>
        >>> progress = client.execute_with_progress(
        ...     'LONG AND COMPLICATED QUERY'
        ... )
        >>>
        >>> timeout = 20
        >>> started_at = datetime.now()
        >>>
        >>> for num_rows, total_rows in progress:
        ...     if total_rows:
        ...         done = float(num_rows) / total_rows
        ...     else:
        ...         done = total_rows
        ...
        ...     now = datetime.now()
        ...     elapsed = (now - started_at).total_seconds()
        ...     # Cancel query if it takes more than 20 seconds
        ...     # to process 50% of rows.
        ...     if elapsed > timeout and done < 0.5:
        ...         client.cancel()
        ...         break
        ... else:
        ...     rv = progress.get_result()
        ...     print(rv)
        ...


.. _execute-iter:

Streaming results
-----------------

When you are dealing with large datasets block by block results streaming may be useful:

    .. code-block:: python

        >>> settings = {'max_block_size': 100000}
        >>> rows_gen = client.execute_iter(
        ...     'QUERY WITH MANY ROWS', settings=settings
        ... )
        >>>
        >>> for row in rows_gen:
        ...     print(row)
        ...


Inserting data
--------------

Insert queries in `Native protocol <https://clickhouse.yandex/docs/en/single/index.html#native-interface-tcp>`_
are a little bit tricky because of ClickHouse's columnar nature. And because we're using Python.

INSERT query consists of two parts: query statement and query values. Query values are split into chunks called blocks.
Each block is sent in binary columnar form.

As data in each block is sent in binary we should not serialize into string by
using substitution ``%(a)s`` and then deserialize it back into Python types.

This INSERT would be extremely slow if executed with thousands rows of data:

    .. code-block:: python

        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES (%(a)s), (%(b)s), ...',
        ...     {'a': 1, 'b': 2, ...}
        ... )


To insert data efficiently, provide data separately, and end your statement with a ``VALUES`` clause:

    .. code-block:: python

        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES',
        ...     [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 100}]
        ... )
        4
        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES',
        ...     [[200]]
        ... )
        1
        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES',
        ...     ((x, ) for x in range(5))
        ... )
        5

You can use any iterable yielding lists, tuples or dicts.

If data is not passed, connection will be terminated after a timeout.

    .. code-block:: python

        >>> client.execute('INSERT INTO test (x) VALUES')  # will hang

The following WILL NOT work:

    .. code-block:: python

        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES (%(a)s), (%(b)s)',
        ...     {'a': 1, 'b': 2}
        ... )


Of course for ``INSERT ... SELECT`` queries data is not needed:

    .. code-block:: python

        >>> client.execute(
        ...     'INSERT INTO test (x) '
        ...     'SELECT * FROM system.numbers LIMIT %(limit)s',
        ...     {'limit': 5}
        ... )
        []

ClickHouse will execute this query like a usual ``SELECT`` query.

Inserting data in different formats with ``FORMAT`` clause is not supported.

See :ref:`insert-from-csv-file` if you need to data in custom format.

DDL
---

DDL queries can be executed in the same way SELECT queries are executed:

    .. code-block:: python

        >>> client.execute('DROP TABLE IF EXISTS test')
        []
        >>> client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')
        []


Async and multithreading
------------------------

Every ClickHouse query is assigned an identifier to enable request execution
tracking. However, ClickHouse native protocol is synchronous: all incoming
queries are executed consecutively. Clickhouse-driver does not yet implement
a connection pool.

To utilize ClickHouse's asynchronous capability you should either use multiple
Client instances or implement a queue.

The same thing is applied to multithreading. Queries from different threads
can't use one Client instance with single connection. You should use different
clients for different threads.

However, if you are using DB API for communication with the server each cursor create
its own Client instance. This makes communication thread-safe.
