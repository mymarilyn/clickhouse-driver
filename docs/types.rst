
Supported types
===============

Each ClickHouse type is deserialized to a corresponding Python type when SELECT queries are prepared.
When serializing INSERT queries, clickhouse-driver accepts a broader range of Python types.
The following ClickHouse types are supported by clickhouse-driver:


[U]Int8/16/32/64/128/256
------------------------

INSERT types: :class:`int`, :class:`long`.

SELECT type: :class:`int`.


Float32/64
----------

INSERT types: :class:`float`, :class:`int`, :class:`long`.

SELECT type: :class:`float`.


Date/Date32
-----------

*Date32 support is new in version 0.2.2.*

INSERT types: :class:`~datetime.date`, :class:`~datetime.datetime`.

SELECT type: :class:`~datetime.date`.


DateTime('timezone')/DateTime64('timezone')
-------------------------------------------

*Timezone support is new in version 0.0.11.*
*DateTime64 support is new in version 0.1.3.*

INSERT types: :class:`~datetime.datetime`, :class:`int`, :class:`long`.

Integers are interpreted as seconds without timezone (UNIX timestamps). Integers can be used when
insertion of datetime column is a bottleneck.

SELECT type: :class:`~datetime.datetime`.

Setting `use_client_time_zone <https://clickhouse.yandex/docs/en/single/#datetime>`_ is taken into consideration.

You can cast DateTime column to integers if you are facing performance issues when selecting large amount of rows.

Due to Python's current limitations minimal DateTime64 resolution is one microsecond.


String/FixedString(N)
---------------------

INSERT types: :class:`str`/:func:`basestring <basestring>`, :class:`bytes`. See note below.

SELECT type: :class:`str`/:func:`basestring <basestring>`, :class:`bytes`. See note below.

String column is encoded/decoded with encoding specified by ``strings_encoding`` setting. Default encoding is UTF-8.

You can specify custom encoding:

    .. code-block:: python

        >>> settings = {'strings_encoding': 'cp1251'}
        >>> rows = client.execute(
        ...     'SELECT * FROM table_with_strings',
        ...     settings=settings
        ... )

Encoding is applied to all string fields in query.

String columns can be returned without any decoding. In this case return values are `bytes`:

    .. code-block:: python

        >>> settings = {'strings_as_bytes': True}
        >>> rows = client.execute(
        ...     'SELECT * FROM table_with_strings',
        ...     settings=settings
        ... )


If a column has FixedString type, upon returning from SELECT it may contain trailing zeroes
in accordance with ClickHouse's storage format. Trailing zeroes are stripped by driver for convenience.

During SELECT, if a string cannot be decoded with specified encoding, it will return as :class:`bytes`.

During INSERT, if ``strings_as_bytes`` setting is not specified and string cannot be encoded with encoding,
a ``UnicodeEncodeError`` will be raised.


Enum8/16
--------

INSERT types: :class:`~enum.Enum`, :class:`int`, :class:`long`, :class:`str`/:func:`basestring <basestring>`.

SELECT type: :class:`str`/:func:`basestring <basestring>`.

    .. code-block:: python

        >>> from enum import IntEnum
        >>>
        >>> class MyEnum(IntEnum):
        ...     foo = 1
        ...     bar = 2
        ...
        >>> client.execute('DROP TABLE IF EXISTS test')
        []
        >>> client.execute('''
        ...     CREATE TABLE test
        ...     (
        ...         x Enum8('foo' = 1, 'bar' = 2)
        ...     ) ENGINE = Memory
        ... ''')
        []
        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES',
        ...     [{'x': MyEnum.foo}, {'x': 'bar'}, {'x': 1}]
        ... )
        3
        >>> client.execute('SELECT * FROM test')
        [('foo',), ('bar',), ('foo',)]

Currently clickhouse-driver can't handle empty enum value due to Python's `Enum` mechanics.
Enum member name must be not empty. See `issue`_ and  `workaround`_.

.. _issue: https://github.com/mymarilyn/clickhouse-driver/issues/48
.. _workaround: https://github.com/mymarilyn/clickhouse-driver/issues/48#issuecomment-412480613


Array(T)
--------

INSERT types: :class:`list`, :class:`tuple`.

SELECT type: :class:`list`.

*Versions before 0.1.4:* SELECT type: :class:`tuple`.


    .. code-block:: python

        >>> client.execute('DROP TABLE IF EXISTS test')
        []
        >>> client.execute(
        ...     'CREATE TABLE test (x Array(Int32)) '
        ...     'ENGINE = Memory'
        ... )
        []
        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES',
        ...     [{'x': [10, 20, 30]}, {'x': [11, 21, 31]}]
        ... )
        2
        >>> client.execute('SELECT * FROM test')
        [((10, 20, 30),), ((11, 21, 31),)]


Nullable(T)
-----------

INSERT types: :data:`~types.NoneType`, ``T``.

SELECT type: :data:`~types.NoneType`, ``T``.


Bool
----

INSERT types: :class:`bool`,

SELECT type: :class:`bool`.


UUID
----

INSERT types: :class:`str`/:func:`basestring <basestring>`, :class:`~uuid.UUID`.

SELECT type: :class:`~uuid.UUID`.


Decimal
-------

*New in version 0.0.16.*

INSERT types: :class:`~decimal.Decimal`, :class:`float`, :class:`int`, :class:`long`.

SELECT type: :class:`~decimal.Decimal`.

Supported subtypes:

* Decimal(P, S).
* Decimal32(S).
* Decimal64(S).
* Decimal128(S).
* Decimal256(S). *New in version 0.2.1.*

IPv4/IPv6
---------

*New in version 0.0.19.*

INSERT types: :class:`~ipaddress.IPv4Address`/:class:`~ipaddress.IPv6Address`, :class:`int`, :class:`long`, :class:`str`/:func:`basestring <basestring>`.

SELECT type: :class:`~ipaddress.IPv4Address`/:class:`~ipaddress.IPv6Address`.

    .. code-block:: python

        >>> from ipaddress import IPv4Address, IPv6Address
        >>>
        >>> client.execute('DROP TABLE IF EXISTS test')
        []
        >>> client.execute(
        ...     'CREATE TABLE test (x IPv4) '
        ...     'ENGINE = Memory'
        ... )
        []
        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES', [
        ...     {'x': '192.168.253.42'},
        ...     {'x': 167772161},
        ...     {'x': IPv4Address('192.168.253.42')}
        ... ])
        3
        >>> client.execute('SELECT * FROM test')
        [(IPv4Address('192.168.253.42'),), (IPv4Address('10.0.0.1'),), (IPv4Address('192.168.253.42'),)]
        >>>
        >>> client.execute('DROP TABLE IF EXISTS test')
        []
        >>> client.execute(
        ...     'CREATE TABLE test (x IPv6) '
        ...     'ENGINE = Memory'
        ... )
        []
        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES', [
        ...     {'x': '79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'},
        ...     {'x': IPv6Address('12ff:0000:0000:0000:0000:0000:0000:0001')},
        ...     {'x': b"y\xf4\xe6\x98E\xde\xa5\x9b'e(\xe3\x8d:5\xae"}
        ... ])
        3
        >>> client.execute('SELECT * FROM test')
        [(IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),), (IPv6Address('12ff::1'),), (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),)]
        >>>


LowCardinality(T)
-----------------

*New in version 0.0.20.*

INSERT types: ``T``.

SELECT type: ``T``.


SimpleAggregateFunction(F, T)
-----------------------------

*New in version 0.0.21.*

INSERT types: ``T``.

SELECT type: ``T``.

AggregateFunctions for `AggregatingMergeTree` Engine are not supported.


Tuple(T1, T2, ...)
------------------

*New in version 0.1.4.*

INSERT types: :class:`list`, :class:`tuple`.

SELECT type: :class:`tuple`.


Nested(flatten_nested=1, default)
---------------------------------

Nested type is represented by sequence of arrays when flatten_nested=1. In example below actual
columns for are ``col.name`` and ``col.version``.

    .. code-block:: sql

      :) CREATE TABLE test_nested (col Nested(name String, version UInt16)) Engine = Memory;

      CREATE TABLE test_nested
      (
          `col` Nested(
          name String,
          version UInt16)
      )
      ENGINE = Memory

      Ok.

      0 rows in set. Elapsed: 0.005 sec.

      :) DESCRIBE TABLE test_nested FORMAT TSV;

      DESCRIBE TABLE test_nested
      FORMAT TSV

      col.name	Array(String)
      col.version	Array(UInt16)

      2 rows in set. Elapsed: 0.004 sec.

Inserting data into nested column in ``clickhouse-client``:

    .. code-block:: sql

      :) INSERT INTO test_nested VALUES (['a', 'b', 'c'], [100, 200, 300]);

      INSERT INTO test_nested VALUES

      Ok.

      1 rows in set. Elapsed: 0.003 sec.

Inserting data into nested column with ``clickhouse-driver``:

    .. code-block:: python

      client.execute('INSERT INTO test_nested VALUES', [
          (['a', 'b', 'c'], [100, 200, 300]),
      ])

Nested(flatten_nested=0)
------------------------

Nested type is represented by array of named tuples when flatten_nested=0.

    .. code-block:: sql

      :) SET flatten_nested = 0;

      SET flatten_nested = 0

      Ok.

      0 rows in set. Elapsed: 0.006 sec. 

      :) CREATE TABLE test_nested (col Nested(name String, version UInt16)) Engine = Memory;

      CREATE TABLE test_nested
      (
          `col` Nested(name String, version UInt16)
      )
      ENGINE = Memory

      Ok.

      0 rows in set. Elapsed: 0.005 sec.

      :) DESCRIBE TABLE test_nested FORMAT TSV;

      DESCRIBE TABLE test_nested
      FORMAT TSV

      col	Nested(name String, version UInt16)					

      1 rows in set. Elapsed: 0.004 sec.

Inserting data into nested column in ``clickhouse-client``:

    .. code-block:: sql

      :) INSERT INTO test_nested VALUES ([('a', 100), ('b', 200), ('c', 300)]);

      INSERT INTO test_nested VALUES

      Ok.

      1 rows in set. Elapsed: 0.003 sec.

Inserting data into nested column with ``clickhouse-driver``:

    .. code-block:: python

      client.execute(
          'INSERT INTO test_nested VALUES',
          [([('a', 100), ('b', 200), ('c', 300)]),]
      )
      # or
      client.execute(
          'INSERT INTO test_nested VALUES',
          [{'col': [{'name': 'a', 'version': 100}, {'name': 'b', 'version': 200}, {'name': 'c', 'version': 300}]}]
      )

Map(key, value)
------------------

*New in version 0.2.1.*

INSERT types: :class:`dict`.

SELECT type: :class:`dict`.
