
Miscellaneous
=============

Client configuring from URL
---------------------------

*New in version 0.1.1.*

Client can be configured from the given URL:

    .. code-block:: python

        >>> from clickhouse_driver import Client
        >>> client = Client.from_url(
        ...     'clickhouse://login:password@host:port/database'
        ... )

Port 9000 is default for schema ``clickhouse``, port 9440 is default for schema ``clickhouses``.

Connection to default database:

    .. code-block:: python

        >>> client = Client.from_url('clickhouse://localhost')


Querystring arguments will be passed along to the :meth:`~clickhouse_driver.connection.Connection` class’s initializer:

    .. code-block:: python

        >>> client = Client.from_url(
        ...     'clickhouse://localhost/database?send_logs_level=trace&'
        ...     'client_name=myclient&'
        ...     'compression=lz4'
        ... )

If parameter doesn't match Connection's init signature will be treated as settings parameter.

.. _insert-from-csv-file:

Inserting data from CSV file
----------------------------

Let's assume you have following data in CSV file.

    .. code-block:: shell

        $ cat /tmp/data.csv
        time,order,qty
        2019-08-01 15:23:14,New order1,5
        2019-08-05 09:14:45,New order2,3
        2019-08-13 12:20:32,New order3,7

Data can be inserted into ClickHouse in the following way:


    .. code-block:: python

        >>> from csv import DictReader
        >>> from datetime import datetime
        >>>
        >>> from clickhouse_driver import Client
        >>>
        >>>
        >>> def iter_csv(filename):
        ...     converters = {
        ...         'qty': int,
        ...         'time': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        ...     }
        ...
        ...     with open(filename, 'r') as f:
        ...         reader = DictReader(f)
        ...         for line in reader:
        ...             yield {k: (converters[k](v) if k in converters else v) for k, v in line.items()}
        ...
        >>> client = Client('localhost')
        >>>
        >>> client.execute(
        ...     'CREATE TABLE IF NOT EXISTS data_csv '
        ...     '('
        ...         'time DateTime, '
        ...         'order String, '
        ...         'qty Int32'
        ...     ') Engine = Memory'
        ... )
        >>> []
        >>> client.execute('INSERT INTO data_csv VALUES', iter_csv('/tmp/data.csv'))
        3



Table can be populated with json file in the similar way.


Adding missed settings
----------------------

It's hard to keep package settings in consistent state with ClickHouse
server's. Some settings can be missed if your server is old. But, if setting
is *supported by your server* and missed in the package it can be added by
simple monkey pathing. Just look into ClickHouse server source and pick
corresponding setting type from package or write your own type.

    .. code-block:: python

        >>> from clickhouse_driver.settings.available import settings as available_settings, SettingBool
        >>> from clickhouse_driver import Client
        >>>
        >>> available_settings['allow_suspicious_low_cardinality_types'] = SettingBool
        >>>
        >>> client = Client('localhost', settings={'allow_suspicious_low_cardinality_types': True})
        >>> client.execute('CREATE TABLE test (x LowCardinality(Int32)) Engine = Null')
        []


*New in version 0.1.5.*

Modern ClickHouse servers (20.*+) use text serialization for settings instead of
binary serialization. You don't have to add missed settings manually into
available. Just specify new settings and it will work.

    .. code-block:: python

        >>> client = Client('localhost', settings={'brand_new_setting': 42})
        >>> client.execute('SELECT 1')


Inserting NULL into NOT NULL columns
------------------------------------

*New in version 0.2.4.*

Client option ``input_format_null_as_default`` does the same thing as in
``clickhouse-client``. But in this package it's disabled by default. You should
enable it if you want cast ``None`` value into default value for current type:

    .. code-block:: python

        >>> settings = {'input_format_null_as_default': True}
        >>> client = Client('localhost', settings=settings)


Client revision downgrading
---------------------------

*New in version 0.2.6.*

For various purposes client can be downgraded with ``client_revision``
parameter.

    .. code-block:: python

        >>> from clickhouse_driver import Client, defines
        >>>
        >>> client = Client('localhost', client_revision=defines.DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME)
        >>> client.execute('SELECT version()')
