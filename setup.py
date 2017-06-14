from codecs import open
from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='clickhouse-driver',
    version='0.0.4',

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


        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],

    keywords='ClickHouse db database cloud analytics',

    packages=[
        p.replace('src', 'clickhouse_driver')
        for p in find_packages(exclude=['tests'])
        if p.startswith('src')
    ],
    package_dir={
        'clickhouse_driver': 'src',
    },
    install_requires=[
        'six',
        'enum34'
    ],
    extras_require={
        'quicklz': ['pyquicklz', 'clickhouse-cityhash==1.0.2'],
        'lz4': ['lz4', 'clickhouse-cityhash==1.0.2'],
        'zstd': ['zstd', 'clickhouse-cityhash==1.0.2']
    },
    test_suite='nose.collector',
    tests_require=[
        'nose',
        'pyquicklz', 'lz4', 'zstd',
        'clickhouse-cityhash==1.0.2'
    ],
)
