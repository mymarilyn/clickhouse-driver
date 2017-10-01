from contextlib import contextmanager
import subprocess
from unittest import TestCase

from src.client import Client
from src.util import compat
from tests import log


if compat.PY3:
    import configparser
else:
    import ConfigParser as configparser


file_config = configparser.ConfigParser()
file_config.read(['setup.cfg'])


log.configure(file_config.get('log', 'level'))


class BaseTestCase(TestCase):
    host = file_config.get('db', 'host')
    port = file_config.getint('db', 'port')
    database = file_config.get('db', 'database')
    user = file_config.get('db', 'user')
    password = file_config.get('db', 'password')

    client = None

    @classmethod
    def emit_cli(cls, statement, database=None):
        if database is None:
            database = cls.database

        args = [
            'clickhouse-client',
            '--database', database,
            '--host', cls.host,
            '--port', str(cls.port),
            '--query', str(statement)
        ]

        process = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        output = process.communicate()
        out, err = output

        if err:
            raise RuntimeError(
                'Error during communication. {}'.format(err)
            )

        return out.decode('utf-8')

    def create_client(self, **kwargs):
        return Client(
            self.host, self.port, self.database, self.user, self.password,
            **kwargs
        )

    @classmethod
    def setUpClass(cls):
        cls.emit_cli(
            'DROP DATABASE IF EXISTS {}'.format(cls.database), 'default'
        )
        cls.emit_cli('CREATE DATABASE {}'.format(cls.database), 'default')

        super(BaseTestCase, cls).setUpClass()

    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.client = self.create_client()

    def tearDown(self):
        self.client.disconnect()
        super(BaseTestCase, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        cls.emit_cli('DROP DATABASE {}'.format(cls.database))
        super(BaseTestCase, cls).tearDownClass()

    @contextmanager
    def create_table(self, columns):
        self.emit_cli(
            'CREATE TABLE test ({}) ''ENGINE = Memory'.format(columns)
        )
        try:
            yield
        except Exception:
            raise
        finally:
            self.emit_cli('DROP TABLE test')
