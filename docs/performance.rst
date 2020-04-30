
Performance
===========

This section compares clickhouse-driver performance over Native interface
with TSV and JSONEachRow formats available over HTTP interface.

clickhouse-driver returns already parsed row items in Python data types.
Driver performs all transformation for you.

When you read data over HTTP you may need to cast strings into Python types.


Test data
---------

Sample data for testing is taken from `ClickHouse docs <https://clickhouse.tech/docs>`_.

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

    pip install clickhouse-driver requests

For fast json parsing we'll use ``ujson`` package:

.. code-block:: bash

    pip install ujson

Installed packages: ::

    $ pip freeze
    certifi==2020.4.5.1
    chardet==3.0.4
    clickhouse-driver==0.1.3
    idna==2.9
    pytz==2019.3
    requests==2.23.0
    tzlocal==2.0.0
    ujson==2.0.3
    urllib3==1.25.9

Versions
--------

Machine: Linux ThinkPad-T460 4.4.0-177-generic #207-Ubuntu SMP Mon Mar 16 01:16:10 UTC 2020 x86_64 x86_64 x86_64 GNU/Linux

Python: CPython 3.6.5 (default, May 30 2019, 14:48:31) [GCC 5.4.0 20160609]


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

.. code-block:: python

    import sys
    import requests

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT {}".format(sys.argv[1], sys.argv[2])
    data = requests.get('http://localhost:8123/', params={'query': query})


Parsed rows
^^^^^^^^^^^

Line split into elements will be consider as "parsed" for TSV format (2)

.. code-block:: python

    import sys
    import requests

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT TSV".format(sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    data = [line.decode('utf-8').split('\t') for line in resp.iter_lines(chunk_size=10000)]


Now we cast each element to it's data type (2.5)

.. code-block:: python

    from datetime import date
    import sys
    import requests


    def get_python_type(ch_type):
      if ch_type.startswith('Int') or ch_type.startswith('UInt'):
        return int

      elif ch_type == 'String' or ch_type.startswith('FixedString'):
        return None

      elif ch_type == 'Date':
        return lambda value: date(*[int(x) for x  in value.split('-')])

      raise ValueError(f'Unsupported type: "{ch_type}"')


    resp = requests.get('http://localhost:8123', params={'query': 'describe table perftest.ontime FORMAT TSV'})
    ch_types = [x.split('\t')[1] for x in resp.text.split('\n') if x]
    python_types = [get_python_type(x) for x in ch_types]

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT TSV".format(sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    data = []

    for line in resp.iter_lines(chunk_size=10000):
       data.append([cls(x) if cls else x for x, cls in zip(line.decode('utf-8').split('\t'), python_types)])


JSONEachRow format can be loaded with json loads (3)

.. code-block:: python

    import sys
    import requests
    from ujson import loads

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT JSONEachRow".format(sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    data = [list(loads(line).values()) for line in resp.iter_lines(chunk_size=10000)]


Get fully parsed rows with ``clickhouse-driver`` in Native format (4)

.. code-block:: python

    import sys
    from clickhouse_driver import Client

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
    client = Client.from_url('clickhouse://localhost')

    data = client.execute(query)


Iteration over rows
^^^^^^^^^^^^^^^^^^^

Iteration over TSV (5)

.. code-block:: python

    import sys
    import requests

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT TSV".format(sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    for line in resp.iter_lines(chunk_size=10000):
      line = line.decode('utf-8').split('\t')


Now we cast each element to it's data type (5.5)

.. code-block:: python

    from datetime import date
    import sys
    import requests


    def get_python_type(ch_type):
      if ch_type.startswith('Int') or ch_type.startswith('UInt'):
        return int

      elif ch_type == 'String' or ch_type.startswith('FixedString'):
        return None

      elif ch_type == 'Date':
        return lambda value: date(*[int(x) for x  in value.split('-')])

      raise ValueError(f'Unsupported type: "{ch_type}"')


    resp = requests.get('http://localhost:8123', params={'query': 'describe table perftest.ontime FORMAT TSV'})
    ch_types = [x.split('\t')[1] for x in resp.text.split('\n') if x]
    python_types = [get_python_type(x) for x in ch_types]

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT TSV".format(sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    for line in resp.iter_lines(chunk_size=10000):
       line = [cls(x) if cls else x for x, cls in zip(line.decode('utf-8').split('\t'), python_types)]


Iteration over JSONEachRow (6)

.. code-block:: python

    import sys
    import requests
    from ujson import loads

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT JSONEachRow".format(sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    for line in resp.iter_lines(chunk_size=10000):
      line = list(loads(line).values())


Iteration over rows with ``clickhouse-driver`` in Native format (7)

.. code-block:: python

    import sys
    from clickhouse_driver import Client

    query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
    client = Client.from_url('clickhouse://localhost')

    for row in client.execute_iter(query):
      pass


Iteration over string rows
^^^^^^^^^^^^^^^^^^^^^^^^^^

OK, but what if we need only string columns?

Iteration over TSV (8)

.. code-block:: python

    import sys
    import requests

    cols = [
        'UniqueCarrier', 'Carrier', 'TailNum', 'FlightNum', 'Origin', 'OriginCityName', 'OriginState',
        'OriginStateFips', 'OriginStateName', 'Dest', 'DestCityName', 'DestState', 'DestStateFips',
        'DestStateName', 'DepartureDelayGroups', 'DepTimeBlk', 'ArrTimeBlk', 'CancellationCode',
        'FirstDepTime', 'TotalAddGTime', 'LongestAddGTime', 'DivAirportLandings', 'DivReachedDest',
        'DivActualElapsedTime', 'DivArrDelay', 'DivDistance', 'Div1Airport', 'Div1WheelsOn', 'Div1TotalGTime',
        'Div1LongestGTime', 'Div1WheelsOff', 'Div1TailNum', 'Div2Airport', 'Div2WheelsOn', 'Div2TotalGTime',
        'Div2LongestGTime', 'Div2WheelsOff', 'Div2TailNum', 'Div3Airport', 'Div3WheelsOn', 'Div3TotalGTime',
        'Div3LongestGTime', 'Div3WheelsOff', 'Div3TailNum', 'Div4Airport', 'Div4WheelsOn', 'Div4TotalGTime',
        'Div4LongestGTime', 'Div4WheelsOff', 'Div4TailNum', 'Div5Airport', 'Div5WheelsOn', 'Div5TotalGTime',
        'Div5LongestGTime', 'Div5WheelsOff', 'Div5TailNum'
    ]

    query = "SELECT {} FROM perftest.ontime WHERE FlightDate < '{}' FORMAT TSV".format(', '.join(cols), sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    for line in resp.iter_lines(chunk_size=10000):
      line = line.decode('utf-8').split('\t')


Iteration over JSONEachRow (9)

.. code-block:: python

    import sys
    import requests
    from ujson import loads

    cols = [
        'UniqueCarrier', 'Carrier', 'TailNum', 'FlightNum', 'Origin', 'OriginCityName', 'OriginState',
        'OriginStateFips', 'OriginStateName', 'Dest', 'DestCityName', 'DestState', 'DestStateFips',
        'DestStateName', 'DepartureDelayGroups', 'DepTimeBlk', 'ArrTimeBlk', 'CancellationCode',
        'FirstDepTime', 'TotalAddGTime', 'LongestAddGTime', 'DivAirportLandings', 'DivReachedDest',
        'DivActualElapsedTime', 'DivArrDelay', 'DivDistance', 'Div1Airport', 'Div1WheelsOn', 'Div1TotalGTime',
        'Div1LongestGTime', 'Div1WheelsOff', 'Div1TailNum', 'Div2Airport', 'Div2WheelsOn', 'Div2TotalGTime',
        'Div2LongestGTime', 'Div2WheelsOff', 'Div2TailNum', 'Div3Airport', 'Div3WheelsOn', 'Div3TotalGTime',
        'Div3LongestGTime', 'Div3WheelsOff', 'Div3TailNum', 'Div4Airport', 'Div4WheelsOn', 'Div4TotalGTime',
        'Div4LongestGTime', 'Div4WheelsOff', 'Div4TailNum', 'Div5Airport', 'Div5WheelsOn', 'Div5TotalGTime',
        'Div5LongestGTime', 'Div5WheelsOff', 'Div5TailNum'
    ]

    query = "SELECT {} FROM perftest.ontime WHERE FlightDate < '{}' FORMAT JSONEachRow".format(', '.join(cols), sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    for line in resp.iter_lines(chunk_size=10000):
      line = list(loads(line).values())


Iteration over string rows with ``clickhouse-driver`` in Native format (10)

.. code-block:: python

    import sys
    from clickhouse_driver import Client

    cols = [
        'UniqueCarrier', 'Carrier', 'TailNum', 'FlightNum', 'Origin', 'OriginCityName', 'OriginState',
        'OriginStateFips', 'OriginStateName', 'Dest', 'DestCityName', 'DestState', 'DestStateFips',
        'DestStateName', 'DepartureDelayGroups', 'DepTimeBlk', 'ArrTimeBlk', 'CancellationCode',
        'FirstDepTime', 'TotalAddGTime', 'LongestAddGTime', 'DivAirportLandings', 'DivReachedDest',
        'DivActualElapsedTime', 'DivArrDelay', 'DivDistance', 'Div1Airport', 'Div1WheelsOn', 'Div1TotalGTime',
        'Div1LongestGTime', 'Div1WheelsOff', 'Div1TailNum', 'Div2Airport', 'Div2WheelsOn', 'Div2TotalGTime',
        'Div2LongestGTime', 'Div2WheelsOff', 'Div2TailNum', 'Div3Airport', 'Div3WheelsOn', 'Div3TotalGTime',
        'Div3LongestGTime', 'Div3WheelsOff', 'Div3TailNum', 'Div4Airport', 'Div4WheelsOn', 'Div4TotalGTime',
        'Div4LongestGTime', 'Div4WheelsOff', 'Div4TailNum', 'Div5Airport', 'Div5WheelsOn', 'Div5TotalGTime',
        'Div5LongestGTime', 'Div5WheelsOff', 'Div5TailNum'
    ]

    query = "SELECT {} FROM perftest.ontime WHERE FlightDate < '{}'".format(', '.join(cols), sys.argv[1])
    client = Client.from_url('clickhouse://localhost')

    for row in client.execute_iter(query):
      pass


Iteration over int rows
^^^^^^^^^^^^^^^^^^^^^^^

Iteration over TSV (11)

.. code-block:: python

    import sys
    import requests

    cols = [
        'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'AirlineID', 'OriginAirportID', 'OriginAirportSeqID',
        'OriginCityMarketID', 'OriginWac', 'DestAirportID', 'DestAirportSeqID', 'DestCityMarketID', 'DestWac',
        'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes', 'DepDel15', 'TaxiOut', 'WheelsOff', 'WheelsOn',
        'TaxiIn', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes', 'ArrDel15', 'ArrivalDelayGroups',
        'Cancelled', 'Diverted', 'CRSElapsedTime', 'ActualElapsedTime', 'AirTime', 'Flights', 'Distance',
        'DistanceGroup', 'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay',
        'Div1AirportID', 'Div1AirportSeqID', 'Div2AirportID', 'Div2AirportSeqID', 'Div3AirportID',
        'Div3AirportSeqID', 'Div4AirportID', 'Div4AirportSeqID', 'Div5AirportID', 'Div5AirportSeqID'
    ]

    query = "SELECT {} FROM perftest.ontime WHERE FlightDate < '{}' FORMAT TSV".format(', '.join(cols), sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    for line in resp.iter_lines(chunk_size=10000):
      line = [int(x) for x in line.split(b'\t')]


Iteration over JSONEachRow (12)

.. code-block:: python

    import sys
    import requests
    from ujson import loads

    cols = [
        'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'AirlineID', 'OriginAirportID', 'OriginAirportSeqID',
        'OriginCityMarketID', 'OriginWac', 'DestAirportID', 'DestAirportSeqID', 'DestCityMarketID', 'DestWac',
        'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes', 'DepDel15', 'TaxiOut', 'WheelsOff', 'WheelsOn',
        'TaxiIn', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes', 'ArrDel15', 'ArrivalDelayGroups',
        'Cancelled', 'Diverted', 'CRSElapsedTime', 'ActualElapsedTime', 'AirTime', 'Flights', 'Distance',
        'DistanceGroup', 'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay',
        'Div1AirportID', 'Div1AirportSeqID', 'Div2AirportID', 'Div2AirportSeqID', 'Div3AirportID',
        'Div3AirportSeqID', 'Div4AirportID', 'Div4AirportSeqID', 'Div5AirportID', 'Div5AirportSeqID'
    ]

    query = "SELECT {} FROM perftest.ontime WHERE FlightDate < '{}' FORMAT JSONEachRow".format(', '.join(cols), sys.argv[1])
    resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

    for line in resp.iter_lines(chunk_size=10000):
      line = list(loads(line).values())


Iteration over int rows with ``clickhouse-driver`` in Native format (13)

.. code-block:: python

    import sys
    from clickhouse_driver import Client

    cols = [
        'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'AirlineID', 'OriginAirportID', 'OriginAirportSeqID',
        'OriginCityMarketID', 'OriginWac', 'DestAirportID', 'DestAirportSeqID', 'DestCityMarketID', 'DestWac',
        'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes', 'DepDel15', 'TaxiOut', 'WheelsOff', 'WheelsOn',
        'TaxiIn', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes', 'ArrDel15', 'ArrivalDelayGroups',
        'Cancelled', 'Diverted', 'CRSElapsedTime', 'ActualElapsedTime', 'AirTime', 'Flights', 'Distance',
        'DistanceGroup', 'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay',
        'Div1AirportID', 'Div1AirportSeqID', 'Div2AirportID', 'Div2AirportSeqID', 'Div3AirportID',
        'Div3AirportSeqID', 'Div4AirportID', 'Div4AirportSeqID', 'Div5AirportID', 'Div5AirportSeqID'
    ]

    query = "SELECT {} FROM perftest.ontime WHERE FlightDate < '{}'".format(', '.join(cols), sys.argv[1])
    client = Client.from_url('clickhouse://localhost')

    for row in client.execute_iter(query):
      pass


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
|Naive requests.get TSV (1)        |    0.40 s |    0.67 s |    0.95 s |    1.67 s |    2.52 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get JSON (1)       |    0.61 s |    1.23 s |    2.09 s |    3.52 s |    5.20 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Plain text without parsing: memory**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get TSV (1)        |     49 MB |    107 MB |    165 MB |    322 MB |    488 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|Naive requests.get JSON (1)       |    206 MB |    564 MB |    916 MB |   1.83 GB |   2.83 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Parsed rows: timing**                                                                       |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (2)              |    0.81 s |    1.81 s |    3.09 s |    7.22 s |   11.87 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (2.5)  |    1.78 s |    4.58 s |    7.42 s |   16.12 s |   25.52 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (3)             |    2.14 s |    5.65 s |    9.20 s |   20.43 s |   31.72 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (4)      |    0.73 s |    1.40 s |    2.08 s |    4.03 s |    6.20 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Parsed rows: memory**                                                                       |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (2)              |    171 MB |    462 MB |    753 MB |   1.51 GB |   2.33 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (2.5)  |    135 MB |    356 MB |    576 MB |   1.15 GB |   1.78 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (3)             |    139 MB |    366 MB |    591 MB |   1.18 GB |   1.82 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (4)      |    135 MB |    337 MB |    535 MB |   1.05 GB |   1.62 GB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over rows: timing**                                                               |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (5)              |    0.49 s |    0.99 s |    1.34 s |    2.58 s |    4.00 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (5.5)  |    1.38 s |    3.38 s |    5.40 s |   10.89 s |   16.59 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (6)             |    1.89 s |    4.73 s |    7.63 s |   15.63 s |   24.60 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (7)      |    0.62 s |    1.28 s |    1.93 s |    3.68 s |    5.54 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over rows: memory**                                                               |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (5)              |     19 MB |     19 MB |     19 MB |     19 MB |     19 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV with cast (5.5)  |     19 MB |     19 MB |     19 MB |     19 MB |     19 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (6)             |     20 MB |     20 MB |     20 MB |     20 MB |     20 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (7)      |     56 MB |     70 MB |     71 MB |     71 MB |     71 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over string rows: timing**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (8)              |    0.40 s |    0.67 s |    0.80 s |    1.55 s |    2.18 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (9)             |    1.14 s |    2.64 s |    4.22 s |    8.48 s |   12.96 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (10)     |    0.46 s |    0.91 s |    1.35 s |    2.49 s |    3.67 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over string rows: memory**                                                        |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (8)              |     19 MB |     19 MB |     19 MB |     19 MB |     19 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (9)             |     20 MB |     20 MB |     20 MB |     20 MB |     20 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (10)     |     46 MB |     56 MB |     57 MB |     57 MB |     57 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over int rows: timing**                                                           |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (11)             |    0.84 s |    2.06 s |    3.22 s |    6.27 s |   10.06 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (12)            |    0.95 s |    2.15 s |    3.55 s |    6.93 s |   10.82 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (13)     |    0.43 s |    0.61 s |    0.86 s |    1.53 s |    2.27 s |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|**Iteration over int rows: memory**                                                           |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get TSV (11)             |     19 MB |     19 MB |     19 MB |     19 MB |     19 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|requests.get JSON (12)            |     20 MB |     20 MB |     20 MB |     20 MB |     20 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+
|clickhouse-driver Native (13)     |     41 MB |     48 MB |     48 MB |     48 MB |     49 MB |
+----------------------------------+-----------+-----------+-----------+-----------+-----------+


Conclusion
----------

If you need to get significant number of rows from ClickHouse server **as text** then TSV format is your choice.
See **Iteration over string rows** results.

But if you need to manipulate over python data types then you should take a look on drivers with Native format.
For most data types driver uses binary :func:`~struct.pack` / :func:`~struct.unpack` for serialization / deserialization.
Which is obviously faster than ``cls() for x in lst``. See (2.5) and (5.5).

It doesn't matter which interface to use if you manipulate small amount of rows.
