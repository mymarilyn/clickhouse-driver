ClickHouse Python Driver
========================

ClickHouse Python Driver with native (TCP) interface support.

.. image:: https://img.shields.io/travis/mymarilyn/clickhouse-driver.svg
   :target: https://travis-ci.org/mymarilyn/clickhouse-driver

Features
--------

- Compression support:

  * `QuickLZ <http://www.quicklz.com/>`_ (isn't fully tested)
  * `LZ4/LZ4HC <http://www.lz4.org/>`_
  * `ZSTD <https://facebook.github.io/zstd/>`_
- Basic type support:

  * Float32/64
  * [U]Int8/16/32/64
  * Date/DateTime
  * String


Installation
------------

The package can be installed using ``pip``:

    .. code-block:: bash

       pip install clickhouse-driver

After basic installing you can install extras packages if you need compression
support. Example of LZ4 compression requirements installation:

    .. code-block:: bash

       pip install clickhouse-driver[lz4]

If you are using modern ``pip`` you have to specify
``--process-dependency-links`` option in this way:

    .. code-block:: bash

       pip install clickhouse-driver[lz4] --process-dependency-links

Unfortunately ClickHouse server comes with built-in old version of CityHash
hashing algorithm. That's why we can't use original
`CityHash <http://pypi.python.org/cityhash>`_ package. Downgraded version of
this algorithm is placed in `github repo <https://github.com/xzkostyan/python-cityhash/tree/v1.0.2>`_
and isn't uploded to PyPi. And modern ``pip`` disable installing dependencies
from links.

You also can specify multiple extras by using comma.
Install LZ4 and ZSTD requirements:

    .. code-block:: bash

       pip install clickhouse-driver[lz4,zstd] --process-dependency-links


Usage example:

    .. code-block:: python

        from clickhouse_driver.client import Client

        client = Client('localhost')

        client.execute('SHOW TABLES')

        client.execute('DROP TABLE IF EXISTS test')

        client.execute('CREATE TABLE test (x Int32) ENGINE = Memory')

        client.execute(
            'INSERT INTO test (x) VALUES',
            [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 100}]
        )
        client.execute('INSERT INTO test (x) VALUES', [[200]])

        print(client.execute('SELECT sum(x) FROM test'))

Connection Parameters
---------------------

The first parameter *host* is required. There are some optional parameters:

- *port* is port ClickHouse server is bound to. Default is ``9000``.
- *database* is database connect to. Default is ``'default'``.
- *user*. Default is ``'default'``.
- *password*. Default is ``''`` (no password).
- *client_name*. This name will appear in server logs. Default is ``'pyclient'``.
- *compression*. Whether or not use compression. Default is ``False``.Possible choices:

  * ``True`` is equivalent to ``'lz4'``.
  * ``'quicklz'``.
  * ``'lz4'``.
  * ``'lz4hc'`` high-compression variant of ``'lz4'``.
  * ``'zstd'``.


You can also specify timeouts via:

- *connect_timeout*. Default is ``10`` seconds.
- *send_receive_timeout*. Default is ``300`` seconds.
- *sync_request_timeout*. Default is ``5`` seconds.


License
=======

ClickHouse Python Driver is distributed under the `MIT license
<http://www.opensource.org/licenses/mit-license.php>`_.