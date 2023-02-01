
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
    backports.zoneinfo==0.2.1
    certifi==2022.12.7
    charset-normalizer==3.0.1
    clickhouse-connect==0.5.0
    clickhouse-driver==0.2.5
    idna==3.4
    lz4==4.3.2
    pytz==2022.7.1
    pytz-deprecation-shim==0.1.0.post0
    requests==2.28.2
    tzdata==2022.7
    tzlocal==4.2
    ujson==5.7.0
    urllib3==1.26.14
    zstandard==0.19.0

For ``clickhouse-connect`` we need to turn off compression with
``compress=False`` for elimination decompression overhead. This package also
adds ``LIMIT`` clause to the query by default.
Let's disable it off with ``query_limit=None``.

Versions
--------

Machine: Linux klebedev-ThinkPad-T460 5.15.0-57-generic #63-Ubuntu SMP Thu Nov 24 13:43:17 UTC 2022 x86_64 x86_64 x86_64 GNU/Linux

Python: Python 3.8.12 (default, Apr 13 2022, 21:16:23) [GCC 11.2.0]


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

    for d in 2017-01-04 2017-01-10 2017-01-16 2017-02-01 2017-02-18; do /usr/bin/time -f "%e s / %M kB" python script.py $d; done

Time will measure:

* elapsed real (wall clock) time used by the process, in seconds;
* maximum resident set size of the process during its lifetime, in kilobytes.

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
|Naive requests.get TSV (1)        |    0.35 s |    0.56 s |    0.83 s |    1.15 s |    1.72 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get JSON (1)       |    0.99 s |    1.80 s |    2.77 s |    5.15 s |    7.80 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Plain text without parsing: memory**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get TSV (1)        |     52 MB |    110 MB |    167 MB |    323 MB |    489 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get JSON (1)       |    263 MB |    726 MB |   1.88 GB |   2.42 GB |   3.75 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Parsed rows: timing**                                                                       |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (2)              |    0.83 s |    1.97 s |    3.32 s |    7.90 s |   13.13 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (2.5)  |    1.59 s |    4.31 s |    6.99 s |   15.60 s |   25.89 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (3)             |    2.78 s |    5.55 s |    9.23 s |   21.45 s |   31.50 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (4)      |    0.87 s |    1.50 s |    2.21 s |    4.20 s |    6.32 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (14)           |    0.89 s |    1.72 s |    2.46 s |    4.85 s |    7.19 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Parsed rows: memory**                                                                       |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (2)              |    182 MB |    487 MB |    794 MB |   1.63 GB |   2.51 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (2.5)  |    138 MB |    359 MB |    579 MB |   1.18 GB |   1.82 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (3)             |    136 MB |    351 MB |    565 MB |   1.15 GB |   1.77 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (4)      |    155 MB |    343 MB |    530 MB |   1.04 GB |   1.58 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (14)           |    139 MB |    333 MB |    524 MB |   1.05 GB |   1.61 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over rows: timing**                                                               |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (5)              |    0.48 s |    0.91 s |    1.28 s |    2.57 s |    3.72 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (5.5)  |    1.25 s |    3.05 s |    4.77 s |    9.67 s |   15.04 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (6)             |    1.80 s |    4.48 s |    7.10 s |   14.45 s |   22.17 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (7)      |    0.72 s |    1.38 s |    2.01 s |    3.65 s |    5.45 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (17)           |    0.85 s |    1.62 s |    2.12 s |    4.12 s |    6.05 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over rows: memory**                                                               |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (5)              |     22 MB |     22 MB |     22 MB |     22 MB |     22 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (5.5)  |     22 MB |     22 MB |     22 MB |     22 MB |     22 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (6)             |     24 MB |     24 MB |     24 MB |     24 MB |     24 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (7)      |     91 MB |     93 MB |     93 MB |     94 MB |     94 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (17)           |     68 MB |     68 MB |     68 MB |     68 MB |     68 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over string rows: timing**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (8)              |    0.44 s |    0.57 s |    0.77 s |    1.40 s |    1.94 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (9)             |    1.03 s |    2.46 s |    3.87 s |    7.76 s |   11.96 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (10)     |    0.63 s |    1.06 s |    1.44 s |    2.45 s |    3.57 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (15)           |    0.62 s |    1.13 s |    1.53 s |    2.84 s |    4.00 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over string rows: memory**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (8)              |     22 MB |     22 MB |     22 MB |     22 MB |     22 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (9)             |     24 MB |     24 MB |     24 MB |     24 MB |     24 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (10)     |     77 MB |     79 MB |     79 MB |     79 MB |     79 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (15)           |     60 MB |     60 MB |     60 MB |     60 MB |     60 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over int rows: timing**                                                           |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (11)             |    0.81 s |    1.66 s |    2.61 s |    5.08 s |    7.91 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (12)            |    0.97 s |    2.02 s |    3.29 s |    6.50 s |   10.00 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (13)     |    0.55 s |    0.78 s |    1.02 s |    1.73 s |    2.44 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (16)           |    0.54 s |    0.79 s |    1.01 s |    1.68 s |    2.20 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over int rows: memory**                                                           |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (11)             |     22 MB |     22 MB |     22 MB |     22 MB |     22 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (12)            |     24 MB |     24 MB |     24 MB |     24 MB |     24 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (13)     |     71 MB |     72 MB |     72 MB |     73 MB |     73 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-connect (16)           |     41 MB |     41 MB |     41 MB |     41 MB |     41 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+


Conclusion
----------

If you need to get significant number of rows from ClickHouse server **as text** then TSV format is your choice.
See **Iteration over string rows** results.

But if you need to manipulate over python data types then you should take a look on drivers with Native format.
For most data types driver uses binary :func:`~struct.pack` / :func:`~struct.unpack` for serialization / deserialization.
Which is obviously faster than ``cls() for x in lst``. See (2.5) and (5.5).

It doesn't matter which interface to use if you manipulate small amount of rows.
