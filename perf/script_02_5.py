from datetime import date
import sys
import requests


def get_python_type(ch_type):
    if ch_type.startswith('Int') or ch_type.startswith('UInt'):
        return int

    elif ch_type == 'String' or ch_type.startswith('FixedString'):
        return None

    elif ch_type == 'Date':
        return lambda value: date(*[int(x) for x  in value.split('-')])

    raise ValueError(f'Unsupported type: "{ch_type}"')


resp = requests.get('http://localhost:8123', params={'query': 'describe table perftest.ontime FORMAT TSV'})
ch_types = [x.split('\t')[1] for x in resp.text.split('\n') if x]
python_types = [get_python_type(x) for x in ch_types]

query = "SELECT * FROM perftest.ontime WHERE FlightDate < '{}' FORMAT TSV".format(sys.argv[1])
resp = requests.get('http://localhost:8123/', stream=True, params={'query': query})

data = []

for line in resp.iter_lines(chunk_size=10000):
    data.append([cls(x) if cls else x for x, cls in zip(line.decode('utf-8').split('\t'), python_types)])
