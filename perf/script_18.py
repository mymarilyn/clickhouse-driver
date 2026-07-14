# /// script
# dependencies = [
#     "clickhouse-driver[arrow]",
# ]
# ///
import sys
from clickhouse_driver import Client

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = Client('localhost')

table = client.query_arrow(query)
