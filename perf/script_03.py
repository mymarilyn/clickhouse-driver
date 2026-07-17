# /// script
# dependencies = [
#     "requests==2.34.2",
#     "ujson==5.13.0",
# ]
# ///
import sys
import requests
from ujson import loads

import timing

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT JSONEachRow".format(sys.argv[1])
resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

data = [list(loads(line).values()) for line in resp.iter_lines(chunk_size=10000)]
