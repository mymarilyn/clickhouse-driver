# /// script
# dependencies = [
#     "requests==2.34.2",
# ]
# ///
import sys
import requests

import timing

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT TSV".format(sys.argv[1])
resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

for line in resp.iter_lines(chunk_size=10000):
    line = line.decode('utf-8').split('\t')
