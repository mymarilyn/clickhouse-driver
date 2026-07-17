# /// script
# dependencies = [
#     "clickhouse-driver[arrow,numpy]==0.2.11",
# ]
# ///
import sys
from clickhouse_driver import Client

# Imported lazily at query time: preload before the clock starts.
import numpy
import pandas
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
client = Client('localhost', settings={'use_numpy': True})

table = client.query_arrow(query)
