.. _development:

Development
===========

Test configuration
------------------

In ``setup.cfg`` you can find ClickHouse server port, credentials, logging
level and another options than can be tuned during local testing.

Running tests locally
---------------------

Install desired Python version with system package manager/pyenv/another manager.

Install test requirements and build package:

    .. code-block:: bash

        python testsrequire.py && python setup.py develop

You should install cython if you want to change ``*.pyx`` files:

    .. code-block:: bash

        pip install cython

ClickHouse on host machine
^^^^^^^^^^^^^^^^^^^^^^^^^^

Install desired versions of ``clickhouse-server`` and ``clickhouse-client`` on
your machine.

Run tests:

    .. code-block:: bash

        py.test -v

ClickHouse in docker
^^^^^^^^^^^^^^^^^^^^

Create container desired version of ``clickhouse-server``:

    .. code-block:: bash

        docker run --rm -e "TZ=Europe/Moscow" -p 127.0.0.1:9000:9000 --name test-clickhouse-server yandex/clickhouse-server:$VERSION

Create container with the same version of ``clickhouse-client``:

    .. code-block:: bash

        docker run --rm --entrypoint "/bin/sh" --name test-clickhouse-client --link test-clickhouse-server:clickhouse-server yandex/clickhouse-client:$VERSION -c 'while :; do sleep 1; done'

Create ``clickhouse-client`` script on your host machine:

    .. code-block:: bash

        echo -e '#!/bin/bash\n\ndocker exec -e "`env | grep ^TZ=`" test-clickhouse-client clickhouse-client "$@"' | sudo tee /usr/local/bin/clickhouse-client > /dev/null
        sudo chmod +x /usr/local/bin/clickhouse-client

After it container ``test-clickhouse-client`` will communicate with
``test-clickhouse-server`` transparently from host machine.

Set ``host=clickhouse-server`` in ``setup.cfg``.

Add entry in hosts file:

    .. code-block:: bash

        echo '127.0.0.1 clickhouse-server' | sudo tee -a /etc/hosts > /dev/null

Set ``TZ=UTC`` and run tests:

    .. code-block:: bash

        export TZ=UTC
        py.test -v

GitHub Actions in forked repository
-----------------------------------

Workflows in forked repositories can be used for running tests.

Workflows don't run in forked repositories by default.
You must enable GitHub Actions in the **Actions** tab of the forked repository.
