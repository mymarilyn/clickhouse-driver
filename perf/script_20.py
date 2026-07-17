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

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = clickhouse_connect.get_client(host='localhost', query_limit=None, compress=False)

table = client.query_arrow(query, settings={'output_format_arrow_string_as_string': 1})
