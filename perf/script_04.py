import sys
from clickhouse_driver import Client

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}'".format(sys.argv[1])
client = Client.from_url('clickhouse://localhost')

data = client.execute(query)
