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

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = Client('localhost', settings={'use_numpy': True})

table = client.query_arrow(query)
