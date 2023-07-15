import sys
import clickhouse_connect

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = clickhouse_connect.get_client(host='localhost', query_limit=None, compress=False)

rv = client.query(query)
with rv:
    for row in rv.stream_rows():
        pass
