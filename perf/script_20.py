# /// script
# dependencies = [
#     "clickhouse-connect",
#     "pyarrow",
# ]
# ///
import sys
import clickhouse_connect

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = clickhouse_connect.get_client(host='localhost', query_limit=None, compress=False)

table = client.query_arrow(query, settings={'output_format_arrow_string_as_string': 1})
