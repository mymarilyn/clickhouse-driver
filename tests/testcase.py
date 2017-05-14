from contextlib import contextmanager
import subprocess

from six import PY3

from unittest import TestCase

from src.client import Client


if PY3:
    import configparser
else:
    import ConfigParser as configparser


file_config = configparser.ConfigParser()
file_config.read(['setup.cfg'])


class BaseTestCase(TestCase):
    host = file_config.get('db', 'host')
    port = int(file_config.get('db', 'port'))
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
            '--query', "{}".format(statement)
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

        return out.strip().decode('utf-8')

    @classmethod
    def create_client(cls):
        return Client(cls.host, cls.port, cls.database, cls.user, cls.password)

    @classmethod
    def setUpClass(cls):
        cls.emit_cli(
            'DROP DATABASE IF EXISTS {}'.format(cls.database), 'default'
        )
        cls.emit_cli('CREATE DATABASE {}'.format(cls.database), 'default')

        cls.client = cls.create_client()

        super(BaseTestCase, cls).setUpClass()

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
