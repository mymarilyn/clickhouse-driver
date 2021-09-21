import configparser
from contextlib import contextmanager
import subprocess
from unittest import TestCase

from clickhouse_driver.client import Client
from tests import log
from tests.util import skip_by_server_version


file_config = configparser.ConfigParser()
file_config.read(['setup.cfg'])


log.configure(file_config.get('log', 'level'))


class BaseTestCase(TestCase):
    required_server_version = None
    server_version = None

    clickhouse_client_binary = file_config.get('db', 'client')
    host = file_config.get('db', 'host')
    port = file_config.getint('db', 'port')
    database = file_config.get('db', 'database')
    user = file_config.get('db', 'user')
    password = file_config.get('db', 'password')

    client = None
    client_kwargs = None
    cli_client_kwargs = None

    @classmethod
    def emit_cli(cls, statement, database=None, encoding='utf-8', **kwargs):
        if database is None:
            database = cls.database

        args = [
            cls.clickhouse_client_binary,
            '--database', database,
            '--host', cls.host,
            '--port', str(cls.port),
            '--query', str(statement)
        ]

        for key, value in kwargs.items():
            args.extend(['--' + key, str(value)])

        process = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        output = process.communicate()
        out, err = output

        if err:
            raise RuntimeError(
                'Error during communication. {}'.format(err)
            )

        return out.decode(encoding)

    def _create_client(self, **kwargs):
        client_kwargs = {
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }
        client_kwargs.update(kwargs)
        return Client(self.host, **client_kwargs)

    def created_client(self, **kwargs):
        return self._create_client(**kwargs)

    @classmethod
    def setUpClass(cls):
        cls.emit_cli(
            'DROP DATABASE IF EXISTS {}'.format(cls.database), 'default'
        )
        cls.emit_cli('CREATE DATABASE {}'.format(cls.database), 'default')

        version_str = cls.emit_cli('SELECT version()').strip()
        cls.server_version = tuple(int(x) for x in version_str.split('.'))

        super(BaseTestCase, cls).setUpClass()

    def setUp(self):
        super(BaseTestCase, self).setUp()

        required = self.required_server_version

        if required and required > self.server_version:
            skip_by_server_version(self, self.required_server_version)

        if callable(self.client_kwargs):
            client_kwargs = self.client_kwargs(self.server_version)
        else:
            client_kwargs = self.client_kwargs
        client_kwargs = client_kwargs or {}
        self.client = self._create_client(**client_kwargs)

    def tearDown(self):
        self.client.disconnect()
        super(BaseTestCase, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        cls.emit_cli('DROP DATABASE {}'.format(cls.database))
        super(BaseTestCase, cls).tearDownClass()

    @contextmanager
    def create_table(self, columns, **kwargs):
        if self.cli_client_kwargs:
            if callable(self.cli_client_kwargs):
                cli_client_kwargs = self.cli_client_kwargs()
                if cli_client_kwargs:
                    kwargs.update(cli_client_kwargs)
            else:
                kwargs.update(self.cli_client_kwargs)

        self.emit_cli(
            'CREATE TABLE test ({}) ''ENGINE = Memory'.format(columns),
            **kwargs
        )
        try:
            yield
        except Exception:
            raise
        finally:
            self.emit_cli('DROP TABLE test')
