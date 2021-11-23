.. _installation:

Installation
============

Python Version
--------------

Clickhouse-driver supports Python 3.4 and newer and PyPy.

Build Dependencies
------------------

Starting from version *0.1.0* for building from source `gcc`, python and linux headers are required.

Example for `python:alpine` docker image:

    .. code-block:: bash

       apk add gcc musl-dev

By default there are wheels for Linux, Mac OS X and Windows.

Packages for Linux and Mac OS X are available for python: 3.6 -- 3.10.

Packages for Windows are available for python: 3.6 -- 3.10.

Starting from version *0.2.3* there are wheels for musl-based Linux distributions.

Dependencies
------------

These distributions will be installed automatically when installing clickhouse-driver.

* `pytz`_ library for timezone calculations.
* `enum34`_ backported Python 3.4 Enum.

.. _pytz: http://pytz.sourceforge.net/
.. _enum34: https://pypi.org/project/enum34/

Optional dependencies
~~~~~~~~~~~~~~~~~~~~~

These distributions will not be installed automatically. Clickhouse-driver will detect and
use them if you install them.

* `clickhouse-cityhash`_ provides CityHash algorithm of specific version, see :ref:`compression-cityhash-notes`.
* `lz4`_ enables `LZ4/LZ4HC compression <http://www.lz4.org/>`_ support.
* `zstd`_ enables `ZSTD compression <https://facebook.github.io/zstd/>`_ support.

.. _clickhouse-cityhash: https://pythonhosted.org/blinker/
.. _lz4: https://python-lz4.readthedocs.io/
.. _zstd: https://pypi.org/project/zstd/


.. _installation-pypi:

Installation from PyPI
----------------------

The package can be installed using ``pip``:

    .. code-block:: bash

       pip install clickhouse-driver

You can install extras packages if you need compression support. Example of
LZ4 compression requirements installation:

    .. code-block:: bash

       pip install clickhouse-driver[lz4]

You also can specify multiple extras by using comma.
Install LZ4 and ZSTD requirements:

    .. code-block:: bash

       pip install clickhouse-driver[lz4,zstd]


.. _installation-numpy-support:

NumPy support
-------------

You can install additional packages (NumPy and Pandas) if you need NumPy support:

    .. code-block:: bash

       pip install clickhouse-driver[numpy]

NumPy supported versions are limited by ``numpy`` package python support.


Installation from github
------------------------

Development version can be installed directly from github:

    .. code-block:: bash

       pip install git+https://github.com/mymarilyn/clickhouse-driver@master#egg=clickhouse-driver
