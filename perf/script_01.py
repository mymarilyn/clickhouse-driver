# /// script
# dependencies = [
#     "requests==2.34.2",
# ]
# ///
import sys
import requests

import timing

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT {}".format(sys.argv[1], sys.argv[2])
data = requests.get('http://localhost:8123/', params={'query': query})
