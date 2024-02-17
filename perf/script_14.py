import sys
import clickhouse_connect

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = clickhouse_connect.get_client(host='localhost', query_limit=None, compress=False)

data = client.query(query).result_rows
