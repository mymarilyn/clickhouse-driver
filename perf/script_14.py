# /// script
# dependencies = [
#     "clickhouse-connect==1.4.2",
# ]
# ///
import sys
import clickhouse_connect

import timing

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = clickhouse_connect.get_client(host='localhost', query_limit=None, compress=False)

data = client.query(query).result_rows
