ClickHouse Python Driver
========================

ClickHouse Python Driver with native (TCP) interface support.

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
=====================

The first parameter *host* is required. There are some optional parameters:

- *port* is port ClickHouse server is bound to. Default is ``9000``.
- *database* is database connect to. Default is ``default``.
- *user*. Default is ``default``.
- *password*. Default is '' (no password).
- *client_name*. This name will appear in server logs. Default is ``pyclient``.

You can also specify timeouts via:

- *connect_timeout*. Default is ``10`` seconds.
- *send_receive_timeout*. Default is ``300`` seconds.
- *sync_request_timeout*. Default is ``5`` seconds.
