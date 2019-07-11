.. _installation:

Installation
============

Python Version
--------------

Clickhouse-driver supports Python 3.4 and newer, Python 2.7, and PyPy.

Build Dependencies
------------------

Starting from version *0.1.0* for building from source `gcc`, python and linux headers are required.

Example for `python:alpine` docker image:

    .. code-block:: bash

       apk add gcc musl-dev

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


Installation from github
------------------------

Development version can be installed directly from github:

    .. code-block:: bash

       pip install git+https://github.com/mymarilyn/clickhouse-driver@master#egg=clickhouse-driver
