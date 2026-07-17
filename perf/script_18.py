# /// script
# dependencies = [
#     "clickhouse-driver[arrow]==0.2.11",
# ]
# ///
import sys
from clickhouse_driver import Client

# Imported lazily at query time: preload before the clock starts.
import pyarrow

import timing

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = Client('localhost')

table = client.query_arrow(query)
