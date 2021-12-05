import os
import sys

USE_NUMPY = bool(os.getenv('USE_NUMPY', False))

tests_require = [
    'parameterized',
    'freezegun',
    'zstd',
    'clickhouse-cityhash>=1.0.2.1'
]

if sys.version_info[0:2] == (3, 4):
    tests_require.append('pytest<5')
else:
    tests_require.append('pytest')

if sys.implementation.name == 'pypy':
    tests_require.append('lz4<=3.0.1')
else:
    tests_require.append('lz4')

if USE_NUMPY:
    tests_require.extend(['numpy', 'pandas'])

try:
    from pip import main as pipmain
except ImportError:
    from pip._internal import main as pipmain

pipmain(['install'] + tests_require)
