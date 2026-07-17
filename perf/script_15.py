# /// script
# dependencies = [
#     "clickhouse-connect==1.4.2",
# ]
# ///
import sys
import clickhouse_connect

import timing

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
client = clickhouse_connect.get_client(host='localhost', query_limit=None, compress=False)

with client.query_rows_stream(query) as stream:
    for row in stream:
        pass
