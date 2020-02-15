from __future__ import unicode_literals

from clickhouse_driver import errors
from ipaddress import IPv6Address, IPv4Address

from tests.testcase import BaseTestCase


class IPv4TestCase(BaseTestCase):
    required_server_version = (19, 3, 3)

    def test_simple(self):
        with self.create_table('a IPv4'):
            data = [
                (IPv4Address("10.0.0.1"),),
                (IPv4Address("192.168.253.42"),)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, (
                '10.0.0.1\n'
                '192.168.253.42\n'
            ))
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv4Address("10.0.0.1"),),
                (IPv4Address("192.168.253.42"),)
            ])

    def test_from_int(self):
        with self.create_table('a IPv4'):
            data = [
                (167772161,),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, (
                '10.0.0.1\n'
            ))
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv4Address("10.0.0.1"),),
            ])

    def test_from_str(self):
        with self.create_table('a IPv4'):
            data = [
                ("10.0.0.1",),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, (
                '10.0.0.1\n'
            ))
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv4Address("10.0.0.1"),),
            ])

    def test_type_mismatch(self):
        data = [(1025.2147,)]
        with self.create_table('a IPv4'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

    def test_bad_ipv4(self):
        data = [('985.512.12.0',)]
        with self.create_table('a IPv4'):
            with self.assertRaises(errors.CannotParseDomainError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_bad_ipv4_with_type_check(self):
        data = [('985.512.12.0',)]
        with self.create_table('a IPv4'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

    def test_nullable(self):
        with self.create_table('a Nullable(IPv4)'):
            data = [(IPv4Address('10.10.10.10'),), (None,)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted,
                             '10.10.10.10\n\\N\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)


class IPv6TestCase(BaseTestCase):
    required_server_version = (19, 3, 3)

    def test_simple(self):
        with self.create_table('a IPv6'):
            data = [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
                (IPv6Address('a22:cc64:cf47:1653:4976:3c0c:ff8d:417c'),),
                (IPv6Address('12ff:0000:0000:0000:0000:0000:0000:0001'),)
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, (
                '79f4:e698:45de:a59b:2765:28e3:8d3a:35ae\n'
                'a22:cc64:cf47:1653:4976:3c0c:ff8d:417c\n'
                '12ff::1\n'
            ))
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
                (IPv6Address('a22:cc64:cf47:1653:4976:3c0c:ff8d:417c'),),
                (IPv6Address('12ff::1'),)
            ])

    def test_from_str(self):
        with self.create_table('a IPv6'):
            data = [
                ('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae',),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, (
                '79f4:e698:45de:a59b:2765:28e3:8d3a:35ae\n'
            ))
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
            ])

    def test_from_bytes(self):
        with self.create_table('a IPv6'):
            data = [
                (b"y\xf4\xe6\x98E\xde\xa5\x9b'e(\xe3\x8d:5\xae",),
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data, types_check=True
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, (
                '79f4:e698:45de:a59b:2765:28e3:8d3a:35ae\n'
            ))
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
            ])

    def test_type_mismatch(self):
        data = [(1025.2147,)]
        with self.create_table('a IPv6'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

    def test_bad_ipv6(self):
        data = [("ghjk:e698:45de:a59b:2765:28e3:8d3a:zzzz",)]
        with self.create_table('a IPv6'):
            with self.assertRaises(errors.CannotParseDomainError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_bad_ipv6_with_type_check(self):
        data = [("ghjk:e698:45de:a59b:2765:28e3:8d3a:zzzz",)]
        with self.create_table('a IPv6'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )

    def test_nullable(self):
        with self.create_table('a Nullable(IPv6)'):
            data = [
                (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
                (None,)]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted,
                             '79f4:e698:45de:a59b:2765:28e3:8d3a:35ae\n\\N\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
