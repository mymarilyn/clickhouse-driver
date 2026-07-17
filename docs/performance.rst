
Performance
===========

This section compares clickhouse-driver performance over Native interface
with TSV and JSONEachRow formats available over HTTP interface.

clickhouse-driver returns already parsed row items in Python data types.
Driver performs all transformation for you.

When you read data over HTTP you may need to cast strings into Python types.


Test data
---------

Sample data for testing is taken from `ClickHouse docs <https://clickhouse.com/docs/en/getting-started/example-datasets/ontime>`_.

Create database and table:

.. code-block:: sql

    DROP DATABASE IF EXISTS perftest;

    CREATE DATABASE perftest;

    CREATE TABLE perftest.ontime (
      Year UInt16,
      Quarter UInt8,
      Month UInt8,
      DayofMonth UInt8,
      DayOfWeek UInt8,
      FlightDate Date,
      UniqueCarrier FixedString(7),
      AirlineID Int32,
      Carrier FixedString(2),
      TailNum String,
      FlightNum String,
      OriginAirportID Int32,
      OriginAirportSeqID Int32,
      OriginCityMarketID Int32,
      Origin FixedString(5),
      OriginCityName String,
      OriginState FixedString(2),
      OriginStateFips String,
      OriginStateName String,
      OriginWac Int32,
      DestAirportID Int32,
      DestAirportSeqID Int32,
      DestCityMarketID Int32,
      Dest FixedString(5),
      DestCityName String,
      DestState FixedString(2),
      DestStateFips String,
      DestStateName String,
      DestWac Int32,
      CRSDepTime Int32,
      DepTime Int32,
      DepDelay Int32,
      DepDelayMinutes Int32,
      DepDel15 Int32,
      DepartureDelayGroups String,
      DepTimeBlk String,
      TaxiOut Int32,
      WheelsOff Int32,
      WheelsOn Int32,
      TaxiIn Int32,
      CRSArrTime Int32,
      ArrTime Int32,
      ArrDelay Int32,
      ArrDelayMinutes Int32,
      ArrDel15 Int32,
      ArrivalDelayGroups Int32,
      ArrTimeBlk String,
      Cancelled UInt8,
      CancellationCode FixedString(1),
      Diverted UInt8,
      CRSElapsedTime Int32,
      ActualElapsedTime Int32,
      AirTime Int32,
      Flights Int32,
      Distance Int32,
      DistanceGroup UInt8,
      CarrierDelay Int32,
      WeatherDelay Int32,
      NASDelay Int32,
      SecurityDelay Int32,
      LateAircraftDelay Int32,
      FirstDepTime String,
      TotalAddGTime String,
      LongestAddGTime String,
      DivAirportLandings String,
      DivReachedDest String,
      DivActualElapsedTime String,
      DivArrDelay String,
      DivDistance String,
      Div1Airport String,
      Div1AirportID Int32,
      Div1AirportSeqID Int32,
      Div1WheelsOn String,
      Div1TotalGTime String,
      Div1LongestGTime String,
      Div1WheelsOff String,
      Div1TailNum String,
      Div2Airport String,
      Div2AirportID Int32,
      Div2AirportSeqID Int32,
      Div2WheelsOn String,
      Div2TotalGTime String,
      Div2LongestGTime String,
      Div2WheelsOff String,
      Div2TailNum String,
      Div3Airport String,
      Div3AirportID Int32,
      Div3AirportSeqID Int32,
      Div3WheelsOn String,
      Div3TotalGTime String,
      Div3LongestGTime String,
      Div3WheelsOff String,
      Div3TailNum String,
      Div4Airport String,
      Div4AirportID Int32,
      Div4AirportSeqID Int32,
      Div4WheelsOn String,
      Div4TotalGTime String,
      Div4LongestGTime String,
      Div4WheelsOff String,
      Div4TailNum String,
      Div5Airport String,
      Div5AirportID Int32,
      Div5AirportSeqID Int32,
      Div5WheelsOn String,
      Div5TotalGTime String,
      Div5LongestGTime String,
      Div5WheelsOff String,
      Div5TailNum String
    ) ENGINE = MergeTree
    PARTITION BY Year
    ORDER BY (Carrier, FlightDate)
    SETTINGS index_granularity = 8192;


Download some data for 2017 year:

.. code-block:: bash

    for s in `seq 2017 2017`
    do
    for m in `seq 1 12`
    do
    wget https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${s}_${m}.zip
    done
    done

Insert data into ClickHouse:

.. code-block:: bash

    for i in *.zip; do echo $i; unzip -cq $i '*.csv' | sed 's/\.00//g' | clickhouse-client --query="INSERT INTO perftest.ontime FORMAT CSVWithNames"; done


Required packages
-----------------

.. code-block:: bash

    pip install clickhouse-driver requests clickhouse-connect

For fast json parsing we'll use ``ujson`` package:

.. code-block:: bash

    pip install ujson

Installed packages: ::

    $ pip freeze
    certifi==2026.5.20
    clickhouse-cityhash==1.0.2.5
    clickhouse-connect==1.4.2
    clickhouse-driver==0.2.11
    lz4==4.4.5
    numpy==2.4.6
    pandas==3.0.3
    pyarrow==25.0.0
    pytz==2026.2
    requests==2.34.2
    tzlocal==5.3.1
    ujson==5.13.0
    urllib3==2.7.0
    zstd==1.5.7.3

For ``clickhouse-connect`` we need to turn off compression with
``compress=False`` for elimination decompression overhead. This package also
adds ``LIMIT`` clause to the query by default.
Let's disable it off with ``query_limit=None``.

Versions
--------

Machine: Apple M2 Pro, 32 GiB RAM, macOS 15.1.1

ClickHouse server: 25.12.11.4 Docker image, ran locally

Python: Python 3.11.11 (CPython, arm64)


Benchmarking
------------

Let's pick number of rows for testing with ``clickhouse-client``.

.. code-block:: sql

    SELECT count() FROM ontime WHERE FlightDate < '2017-01-04'

    45202

.. code-block:: sql

    SELECT count() FROM ontime WHERE FlightDate < '2017-01-10'

    131848

.. code-block:: sql

    SELECT count() FROM ontime WHERE FlightDate < '2017-01-16'

    217015

.. code-block:: sql

    SELECT count() FROM ontime WHERE FlightDate < '2017-02-01'

    450017

.. code-block:: sql

    SELECT count() FROM ontime WHERE FlightDate < '2017-02-18'

    697813

Scripts below can be benchmarked with following one-liner:

.. code-block:: bash

    for d in 2017-01-04 2017-01-10 2017-01-16 2017-02-01 2017-02-18; do python perf/script.py $d; done

Each script ends its imports with ``import timing``: the clock starts
there and the result is printed on process exit. Measured are:

* elapsed real (wall clock) time, in seconds — interpreter startup and
  imports are excluded;
* maximum resident set size of the process during its lifetime —
  includes the imported packages.

.. literalinclude:: ../perf/timing.py
    :language: python

Plain text without parsing
^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's take get plain text response from ClickHouse server as baseline.


Fetching not parsed data with pure requests (1)

.. literalinclude:: ../perf/script_01.py
    :language: python


Parsed rows
^^^^^^^^^^^

Line split into elements will be consider as "parsed" for TSV format (2)

.. literalinclude:: ../perf/script_02.py
    :language: python

Now we cast each element to it's data type (2.5)

.. literalinclude:: ../perf/script_02_5.py
    :language: python

JSONEachRow format can be loaded with json loads (3)

.. literalinclude:: ../perf/script_03.py
    :language: python

Get fully parsed rows with ``clickhouse-driver`` in Native format (4)

.. literalinclude:: ../perf/script_04.py
    :language: python

Get fully parsed rows with ``clickhouse-connect`` (14)

.. literalinclude:: ../perf/script_14.py
    :language: python


Iteration over rows
^^^^^^^^^^^^^^^^^^^

Iteration over TSV (5)

.. literalinclude:: ../perf/script_05.py
    :language: python

Now we cast each element to it's data type (5.5)

.. literalinclude:: ../perf/script_05_5.py
    :language: python

Iteration over JSONEachRow (6)

.. literalinclude:: ../perf/script_06.py
    :language: python

Iteration over rows with ``clickhouse-driver`` in Native format (7)

.. literalinclude:: ../perf/script_07.py
    :language: python

Iteration over rows with ``clickhouse-connect`` (17)

.. literalinclude:: ../perf/script_17.py
    :language: python


Iteration over string rows
^^^^^^^^^^^^^^^^^^^^^^^^^^

OK, but what if we need only string columns?

Iteration over TSV (8)

.. literalinclude:: ../perf/script_08.py
    :language: python

Iteration over JSONEachRow (9)

.. literalinclude:: ../perf/script_09.py
    :language: python

Iteration over string rows with ``clickhouse-driver`` in Native format (10)

.. literalinclude:: ../perf/script_10.py
    :language: python

Iteration over string rows with ``clickhouse-connect`` (15)

.. literalinclude:: ../perf/script_15.py
    :language: python


Iteration over int rows
^^^^^^^^^^^^^^^^^^^^^^^

Iteration over TSV (11)

.. literalinclude:: ../perf/script_11.py
    :language: python

Iteration over JSONEachRow (12)

.. literalinclude:: ../perf/script_12.py
    :language: python

Iteration over int rows with ``clickhouse-driver`` in Native format (13)

.. literalinclude:: ../perf/script_13.py
    :language: python

Iteration over int rows with ``clickhouse-connect`` (16)

.. literalinclude:: ../perf/script_16.py
    :language: python


PyArrow tables
^^^^^^^^^^^^^^

*New in version 0.2.11.*

Get query result as PyArrow Table with ``clickhouse-driver`` in Native
format (18)

.. literalinclude:: ../perf/script_18.py
    :language: python

The same with NumPy fast paths enabled (19)

.. literalinclude:: ../perf/script_19.py
    :language: python

Get query result as PyArrow Table with ``clickhouse-connect`` (20)

.. literalinclude:: ../perf/script_20.py
    :language: python


PyArrow tables over typed columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Analytic extracts usually select a typed subset of columns instead of
``SELECT *``. The same int and string column projections as in the
iteration sections above, as PyArrow tables.

Int columns with ``clickhouse-driver``, ``use_numpy`` (21)

.. literalinclude:: ../perf/script_21.py
    :language: python

Int columns with ``clickhouse-connect`` (22)

.. literalinclude:: ../perf/script_22.py
    :language: python

String columns with ``clickhouse-driver``, ``use_numpy`` (23)

.. literalinclude:: ../perf/script_23.py
    :language: python

String columns with ``clickhouse-connect`` (24)

.. literalinclude:: ../perf/script_24.py
    :language: python


Results
-------

This table contains memory and timing benchmark results of snippets above.

JSON in table is shorthand for JSONEachRow.

.. rst-class:: table-small-text table-center-header table-right-text-align-results

+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|                                  |                            Rows                           |
|                                  +-----------+-----------+-----------+-----------+-----------+
|                                  |    50k    |    131k   |    217k   |    450k   |    697k   |
+==================================+===========+===========+===========+===========+===========+
|**Plain text without parsing: timing**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get TSV (1)        |    0.13 s |    0.28 s |    0.45 s |    0.80 s |    1.26 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get JSON (1)       |    0.38 s |    1.07 s |    1.81 s |    3.95 s |    7.48 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Plain text without parsing: memory**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get TSV (1)        |     61 MB |    118 MB |    175 MB |    331 MB |    497 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get JSON (1)       |    221 MB |    587 MB |    935 MB |   1.48 GB |   2.84 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Parsed rows: timing**                                                                       |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (2)              |    0.25 s |    0.64 s |    1.31 s |    3.81 s |    6.95 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (2.5)  |    0.67 s |    1.69 s |    3.07 s |    7.29 s |   12.35 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (3)             |    0.82 s |    2.53 s |    3.67 s |    8.05 s |   15.04 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (4)      |    0.18 s |    0.44 s |    0.67 s |    1.46 s |    2.28 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (14)           |    0.20 s |    0.50 s |    0.83 s |    1.70 s |    2.71 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Parsed rows: memory**                                                                       |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (2)              |    183 MB |    473 MB |    765 MB |   1.52 GB |   2.33 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (2.5)  |    142 MB |    358 MB |    536 MB |   1.12 GB |   1.73 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (3)             |    136 MB |    303 MB |    532 MB |   1.05 GB |   1.61 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (4)      |    167 MB |    372 MB |    548 MB |   1.03 GB |   1.47 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (14)           |    172 MB |    384 MB |    577 MB |   1.01 GB |   1.21 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over rows: timing**                                                               |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (5)              |    0.22 s |    0.54 s |    0.80 s |    1.42 s |    2.12 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (5.5)  |    0.52 s |    1.42 s |    2.26 s |    4.59 s |    7.34 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (6)             |    0.72 s |    1.97 s |    3.13 s |    9.36 s |   10.45 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (7)      |    0.23 s |    0.56 s |    0.94 s |    1.98 s |    2.70 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (17)           |    0.23 s |    0.50 s |    0.78 s |    1.61 s |    2.50 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over rows: memory**                                                               |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (5)              |     31 MB |     31 MB |     31 MB |     31 MB |     31 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (5.5)  |     30 MB |     31 MB |     31 MB |     31 MB |     31 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (6)             |     30 MB |     32 MB |     31 MB |     30 MB |     32 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (7)      |    111 MB |    136 MB |    135 MB |    148 MB |    156 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (17)           |    117 MB |    136 MB |    146 MB |    162 MB |    164 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over string rows: timing**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (8)              |    0.11 s |    0.25 s |    0.36 s |    0.68 s |    1.17 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (9)             |    0.44 s |    1.19 s |    1.95 s |    3.35 s |    5.08 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (10)     |    0.11 s |    0.26 s |    0.41 s |    0.82 s |    1.33 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (15)           |    0.14 s |    0.34 s |    0.55 s |    1.23 s |    1.67 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over string rows: memory**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (8)              |     31 MB |     30 MB |     30 MB |     31 MB |     31 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (9)             |     31 MB |     31 MB |     32 MB |     31 MB |     31 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (10)     |     78 MB |     85 MB |     84 MB |     94 MB |     98 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (15)           |     79 MB |    104 MB |    112 MB |    125 MB |    127 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over int rows: timing**                                                           |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (11)             |    0.27 s |    0.69 s |    1.08 s |    2.20 s |    3.40 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (12)            |    0.37 s |    1.00 s |    1.49 s |    2.86 s |    4.51 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (13)     |    0.13 s |    0.20 s |    0.31 s |    0.62 s |    1.00 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (16)           |    0.08 s |    0.15 s |    0.23 s |    0.47 s |    0.67 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over int rows: memory**                                                           |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (11)             |     30 MB |     31 MB |     30 MB |     31 MB |     31 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (12)            |     31 MB |     31 MB |     31 MB |     32 MB |     31 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (13)     |     79 MB |     80 MB |     85 MB |     89 MB |    103 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (16)           |     62 MB |     81 MB |     93 MB |    105 MB |     99 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**PyArrow tables: timing**                                                                    |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (18)     |    0.51 s |    0.72 s |    0.93 s |    2.08 s |    2.96 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver use_numpy (19)  |    0.13 s |    0.34 s |    0.48 s |    0.93 s |    1.41 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (20)           |    0.13 s |    0.25 s |    0.40 s |    0.87 s |    1.28 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**PyArrow tables: memory**                                                                    |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (18)     |    181 MB |    272 MB |    303 MB |    446 MB |    600 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver use_numpy (19)  |    167 MB |    224 MB |    291 MB |    464 MB |    645 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (20)           |     97 MB |    170 MB |    240 MB |    435 MB |    639 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**PyArrow tables, int columns: timing**                                                       |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver use_numpy (21)  |    0.04 s |    0.08 s |    0.12 s |    0.21 s |    0.32 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (22)           |    0.05 s |    0.09 s |    0.14 s |    0.27 s |    0.39 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**PyArrow tables, int columns: memory**                                                       |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver use_numpy (21)  |    130 MB |    153 MB |    176 MB |    243 MB |    310 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (22)           |     74 MB |    106 MB |    136 MB |    218 MB |    306 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**PyArrow tables, string columns: timing**                                                    |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver use_numpy (23)  |    0.09 s |    0.22 s |    0.33 s |    0.83 s |    1.06 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (24)           |    0.09 s |    0.20 s |    0.29 s |    0.59 s |    0.92 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**PyArrow tables, string columns: memory**                                                    |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver use_numpy (23)  |    152 MB |    191 MB |    226 MB |    266 MB |    434 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (24)           |     81 MB |    121 MB |    162 MB |    272 MB |    390 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+


Conclusion
----------

If you need to get significant number of rows from ClickHouse server **as text** then TSV format is your choice.
See **Iteration over string rows** results.

But if you need to manipulate over python data types then you should take a look on drivers with Native format.
For most data types driver uses binary :func:`~struct.pack` / :func:`~struct.unpack` for serialization / deserialization.
Which is obviously faster than ``cls() for x in lst``. See (2.5) and (5.5).

It doesn't matter which interface to use if you manipulate small amount of rows.