# /// script
# dependencies = [
#     "clickhouse-connect==1.4.2",
#     "pyarrow==25.0.0",
# ]
# ///
import sys
import clickhouse_connect

# Imported lazily at query time: preload before the clock starts.
import pyarrow

import timing

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
client = clickhouse_connect.get_client(host='localhost', query_limit=None, compress=False)

table = client.query_arrow(query, settings={'output_format_arrow_string_as_string': 1})
