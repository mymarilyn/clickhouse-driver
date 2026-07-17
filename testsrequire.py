import os
import sys

USE_NUMPY = bool(int(os.getenv('USE_NUMPY') or '0'))
USE_ARROW = bool(int(os.getenv('USE_ARROW') or '0'))

tests_require = [
    'pytest',
    'parameterized',
    'freezegun',
    'zstd',
    'clickhouse-cityhash>=1.0.2.6'
]

if sys.implementation.name == 'pypy':
    tests_require.append('lz4<=3.0.1')
else:
    tests_require.append('lz4')

if USE_NUMPY:
    tests_require.extend(['numpy', 'pandas'])

if USE_ARROW:
    tests_require.append('pyarrow')

try:
    from pip import main as pipmain
except ImportError:
    from pip._internal import main as pipmain

pipmain(['install'] + tests_require)
