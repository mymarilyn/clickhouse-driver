import sys
import requests
from ujson import loads

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT JSONEachRow".format(sys.argv[1])
resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

for line in resp.iter_lines(chunk_size=10000):
  line = list(loads(line).values())
