User's Guide
------------

This part of the documentation focuses on step-by-step instructions for development with clickhouse-driver.

Clickhouse-driver is designed to communicate with ClickHouse server from Python over native protocol.

ClickHouse server provides two protocols for communication:

* HTTP protocol (port 8123 by default);
* Native (TCP) protocol (port 9000 by default).

Each protocol has own advantages and disadvantages. Here we focus on advantages of native protocol:

* Native protocol is more configurable by various settings.
* Binary data transfer is more compact than text data.
* Building python types from binary data is more effective than from text data.
* LZ4 compression is `faster than gzip <https://catchchallenger.first-world.info/wiki/Quick_Benchmark:_Gzip_vs_Bzip2_vs_LZMA_vs_XZ_vs_LZ4_vs_LZO#Compression_time>`_.
  Gzip compression is used in HTTP protocol.
* Query profile info is available over native protocol. We can read rows before limit metric for example.

Once again: clickhouse-driver uses native protocol (port 9000).

There is an asynchronous wrapper for clickhouse-driver: aioch. It's available `here <https://github.com/mymarilyn/aioch>`_.

.. toctree::
   :maxdepth: 2

   installation
   quickstart
   features
   types
   performance
   misc
   unsupportedserverversions

API Reference
-------------

If you are looking for information on a specific function, class or
method, this part of the documentation is for you.

.. toctree::
   :maxdepth: 2

   api
   dbapi

Additional Notes
----------------

Legal information, changelog and contributing are here for the interested.

.. toctree::
   :maxdepth: 2

   development
   changelog
   license
   contributing
