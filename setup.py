import os
import sys
import re
from codecs import open

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))


PY34 = sys.version_info[0:2] >= (3, 4)

install_requires = ['pytz']
if not PY34:
    install_requires.append('enum34')
    install_requires.append('ipaddress')


def read_version():
    regexp = re.compile(r'^VERSION\W*=\W*\(([^\(\)]*)\)')
    init_py = os.path.join(here, 'clickhouse_driver', '__init__.py')
    with open(init_py, encoding='utf-8') as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1).replace(', ', '.')
        else:
            raise RuntimeError(
                'Cannot find version in clickhouse_driver/__init__.py'
            )


with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='clickhouse-driver',
    version=read_version(),

    description='Python driver with native interface for ClickHouse',
    long_description=long_description,

    url='https://github.com/mymarilyn/clickhouse-driver',

    author='Konstantin Lebedev',
    author_email='kostyan.lebedev@gmail.com',

    license='MIT',

    classifiers=[
        'Development Status :: 4 - Beta',


        'Environment :: Console',


        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',


        'License :: OSI Approved :: MIT License',


        'Operating System :: OS Independent',


        'Programming Language :: SQL',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: PyPy',

        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],

    keywords='ClickHouse db database cloud analytics',

    packages=find_packages('.', exclude=['tests*']),
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*',
    install_requires=install_requires,
    extras_require={
        'lz4': ['lz4', 'clickhouse-cityhash>=1.0.2.1'],
        'zstd': ['zstd', 'clickhouse-cityhash>=1.0.2.1']
    },
    test_suite='nose.collector',
    tests_require=[
        'nose',
        'mock',
        'freezegun',
        'lz4', 'zstd',
        'clickhouse-cityhash>=1.0.2.1'
    ],
)
