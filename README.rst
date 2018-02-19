ClickHouse Python Driver
========================

ClickHouse Python Driver with native (TCP) interface support.

.. image:: https://coveralls.io/repos/github/mymarilyn/clickhouse-driver/badge.svg?branch=master
    :target: https://coveralls.io/github/mymarilyn/clickhouse-driver?branch=master

.. image:: https://travis-ci.org/mymarilyn/clickhouse-driver.svg?branch=master
   :target: https://travis-ci.org/mymarilyn/clickhouse-driver

Asynchronous wrapper is available here: https://github.com/mymarilyn/aioch

Features
--------

- Compression support:

  * `LZ4/LZ4HC <http://www.lz4.org/>`_
  * `ZSTD <https://facebook.github.io/zstd/>`_

- TLS support (since server version 1.1.54304).

- Basic types support:

  * Float32/64
  * [U]Int8/16/32/64
  * Date/DateTime
  * String/FixedString(N)
  * Enum8/16
  * Array(T)
  * Nullable(T)
  * UUID

- `External data <https://clickhouse.yandex/docs/en/single/index.html#external-data-for-query-processing>`_ for query processing.

- `Query settings <https://clickhouse.yandex/docs/en/single/index.html#document-operations/settings/index>`_.

- Query progress information.


Installation
------------

The package can be installed using ``pip``:

    .. code-block:: bash

       pip install clickhouse-driver

You can install extras packages if you need compression support. Example of
LZ4 compression requirements installation:

    .. code-block:: bash

       pip install clickhouse-driver[lz4]

You also can specify multiple extras by using comma.
Install LZ4 and ZSTD requirements:

    .. code-block:: bash

       pip install clickhouse-driver[lz4,zstd]


Usage example:

    .. code-block:: python

        from clickhouse_driver import Client

        client = Client('localhost')

        print(client.execute('SHOW TABLES'))

        client.execute('DROP TABLE IF EXISTS test')

        client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')

        client.execute(
            'INSERT INTO test (x) VALUES',
            [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 100}]
        )
        client.execute('INSERT INTO test (x) VALUES', [[200]])

        print(client.execute('SELECT sum(x) FROM test'))

Arrays:

    .. code-block:: python

        client.execute('CREATE TABLE test2 (x Array(Int32)) ENGINE = Memory')
        client.execute(
            'INSERT INTO test2 (x) VALUES',
            [{'x': [10, 20, 30]}, {'x': [11, 21, 31]}]
        )

        print(client.execute('SELECT * FROM test2'))

Enums:

    .. code-block:: python

        from enum import IntEnum

        class MyEnum(IntEnum):
            foo = 1
            bar = 2

        client.execute('''
            CREATE TABLE test3
            (
                x Enum8('foo' = 1, 'bar' = 2)
            ) ENGINE = Memory
        ''')
        client.execute(
            'INSERT INTO test3 (x) VALUES',
            [{'x': MyEnum.foo}, {'x': 'bar'}, {'x': 1}]
        )

        print(client.execute('SELECT * FROM test3'))


Data compression:

    .. code-block:: python

        from clickhouse_driver import Client

        client_with_lz4 = Client('localhost', compression=True)
        client_with_lz4 = Client('localhost', compression='lz4')
        client_with_zstd = Client('localhost', compression='zstd')

Secure connection:

    .. code-block:: python

        from clickhouse_driver import Client

        client = Client('localhost', secure=True)
        # Using self-signed certificate.
        self_signed_client = Client('localhost', secure=True, ca_certs='/etc/clickhouse-server/server.crt')
        # Disable verification.
        no_verifyed_client = Client('localhost', secure=True, verify=False)

        # Example of secured client with Let's Encrypt certificate.
        import certifi

        client = Client('remote-host', secure=True, ca_certs=certifi.where())

External data for query processing:

    .. code-block:: python

        tables = [{
            'name': 'ext',
            'structure': [('x', 'Int32'), ('y', 'Array(Int32)')],
            'data': [
                {'x': 100, 'y': [2, 4, 6, 8]},
                {'x': 500, 'y': [1, 3, 5, 7]},
            ]
        }]
        rv = client.execute(
            'SELECT sum(x) FROM ext', external_tables=tables)
        print(rv)


Query progress information:

    .. code-block:: python

        from datetime import datetime

        progress = client.execute_with_progress('LONG AND COMPLICATED QUERY')

        timeout = 20
        started_at = datetime.now()

        for num_rows, total_rows in progress:
            done = float(num_rows) / total_rows if total_rows else total_rows
            now = datetime.now()
            # Cancel query if it takes more than 20 seconds to process 50% of rows.
            if (now - started_at).total_seconds() > timeout and done < 0.5:
                client.cancel()
                break
        else:
            rv = progress.get_result()
            print(rv)


CityHash algorithm notes
------------------------

Unfortunately ClickHouse server comes with built-in old version of CityHash
hashing algorithm. That's why we can't use original
`CityHash <http://pypi.python.org/cityhash>`_ package. Downgraded version of
this algorithm is placed at `PyPI <https://pypi.python.org/pypi/clickhouse-cityhash>`_.


Client Parameters
-----------------

The first parameter *host* is required. There are some optional parameters:

- *port* is port ClickHouse server is bound to. Default is ``9000``.
- *database* is database connect to. Default is ``'default'``.
- *user*. Default is ``'default'``.
- *password*. Default is ``''`` (no password).
- *client_name*. This name will appear in server logs. Default is ``'python-driver'``.
- *compression*. Whether or not use compression. Default is ``False``. Possible choices:

  * ``True`` is equivalent to ``'lz4'``.
  * ``'lz4'``.
  * ``'lz4hc'`` high-compression variant of ``'lz4'``.
  * ``'zstd'``.
- *insert_block_size*. Chunk size to split rows for ``INSERT``. Default is ``1048576``.

SSL/TLS parameters:

- *secure*. Establish secure connection. Default is ``False``.
- *verify*. Specifies whether a certificate is required and whether it will be validated after connection.
  Default is ``True``.
- other parameters: *ssl_version*, *ca_certs*, *ciphers*.
  See `ssl.wrap_socket <https://docs.python.org/3/library/ssl.html#ssl.wrap_socket>`_ documentation.

You can also specify timeouts via:

- *connect_timeout*. Default is ``10`` seconds.
- *send_receive_timeout*. Default is ``300`` seconds.
- *sync_request_timeout*. Default is ``5`` seconds.


Miscellaneous
-------------

Passing parameters to SELECT SQL queries:

    .. code-block:: python

        from datetime import date

        rv = client.execute(
            'SELECT %(date)s, %(a)s + %(b)s',
            {'date': date.today(), 'a': 1, 'b': 2}
        )
        print(rv)

Specifying `query_id`:

    .. code-block:: python

        from uuid import uuid1

        query_id = str(uuid1())
        print(client.execute('SHOW TABLES', query_id=query_id))

Overriding default query settings:

    .. code-block:: python

        # Set lower priority to query and limit max number threads to execute the request.
        settings = {'max_threads': 2, 'priority': 10}
        print(client.execute('SHOW TABLES', settings=settings))

Retrieving results in columnar form. This is also faster:

    .. code-block:: python

        print(client.execute('SELECT arrayJoin(range(3))', columnar=True))

Data types check is disabled for performance on ``INSERT`` queries.
You can turn it on by *types_check* option:

    .. code-block:: python

        client.execute('INSERT INTO test (x) VALUES', [('abc', )], types_check=True)

License
=======

ClickHouse Python Driver is distributed under the `MIT license
<http://www.opensource.org/licenses/mit-license.php>`_.
