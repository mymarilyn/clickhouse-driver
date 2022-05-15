import ssl

from clickhouse_driver import Client
from clickhouse_driver.compression.lz4 import Compressor as LZ4Compressor
from clickhouse_driver.compression.lz4hc import Compressor as LZHC4Compressor
from clickhouse_driver.compression.zstd import Compressor as ZSTDCompressor
from clickhouse_driver.protocol import Compression
from tests.numpy.util import check_numpy
from tests.testcase import TestCase


class ClientFromUrlTestCase(TestCase):
    def assertHostsEqual(self, client, another, msg=None):
        self.assertEqual(list(client.connection.hosts), another, msg=msg)

    def test_simple(self):
        c = Client.from_url('clickhouse://host')

        self.assertHostsEqual(c, [('host', 9000)])
        self.assertEqual(c.connection.database, '')

        c = Client.from_url('clickhouse://host/db')

        self.assertHostsEqual(c, [('host', 9000)])
        self.assertEqual(c.connection.database, 'db')

    def test_credentials(self):
        c = Client.from_url('clickhouse://host/db')

        self.assertEqual(c.connection.user, 'default')
        self.assertEqual(c.connection.password, '')

        c = Client.from_url('clickhouse://admin:secure@host/db')

        self.assertEqual(c.connection.user, 'admin')
        self.assertEqual(c.connection.password, 'secure')

        c = Client.from_url('clickhouse://user:@host/db')

        self.assertEqual(c.connection.user, 'user')
        self.assertEqual(c.connection.password, '')

    def test_credentials_unquoting(self):
        c = Client.from_url('clickhouse://ad%3Amin:se%2Fcure@host/db')

        self.assertEqual(c.connection.user, 'ad:min')
        self.assertEqual(c.connection.password, 'se/cure')

    def test_schema(self):
        c = Client.from_url('clickhouse://host')
        self.assertFalse(c.connection.secure_socket)

        c = Client.from_url('clickhouses://host')
        self.assertTrue(c.connection.secure_socket)

        c = Client.from_url('test://host')
        self.assertFalse(c.connection.secure_socket)

    def test_port(self):
        c = Client.from_url('clickhouse://host')
        self.assertHostsEqual(c, [('host', 9000)])

        c = Client.from_url('clickhouses://host')
        self.assertHostsEqual(c, [('host', 9440)])

        c = Client.from_url('clickhouses://host:1234')
        self.assertHostsEqual(c, [('host', 1234)])

    def test_secure(self):
        c = Client.from_url('clickhouse://host?secure=n')
        self.assertHostsEqual(c, [('host', 9000)])
        self.assertFalse(c.connection.secure_socket)

        c = Client.from_url('clickhouse://host?secure=y')
        self.assertHostsEqual(c, [('host', 9440)])
        self.assertTrue(c.connection.secure_socket)

        c = Client.from_url('clickhouse://host:1234?secure=y')
        self.assertHostsEqual(c, [('host', 1234)])
        self.assertTrue(c.connection.secure_socket)

        with self.assertRaises(ValueError):
            Client.from_url('clickhouse://host:1234?secure=nonono')

    def test_compression(self):
        c = Client.from_url('clickhouse://host?compression=n')
        self.assertEqual(c.connection.compression, Compression.DISABLED)
        self.assertIsNone(c.connection.compressor_cls)

        c = Client.from_url('clickhouse://host?compression=y')
        self.assertEqual(c.connection.compression, Compression.ENABLED)
        self.assertIs(c.connection.compressor_cls, LZ4Compressor)

        c = Client.from_url('clickhouse://host?compression=lz4')
        self.assertEqual(c.connection.compression, Compression.ENABLED)
        self.assertIs(c.connection.compressor_cls, LZ4Compressor)

        c = Client.from_url('clickhouse://host?compression=lz4hc')
        self.assertEqual(c.connection.compression, Compression.ENABLED)
        self.assertIs(c.connection.compressor_cls, LZHC4Compressor)

        c = Client.from_url('clickhouse://host?compression=zstd')
        self.assertEqual(c.connection.compression, Compression.ENABLED)
        self.assertIs(c.connection.compressor_cls, ZSTDCompressor)

        with self.assertRaises(ValueError):
            Client.from_url('clickhouse://host:1234?compression=custom')

    def test_client_name(self):
        c = Client.from_url('clickhouse://host?client_name=native')
        self.assertEqual(c.connection.client_name, 'ClickHouse native')

    def test_timeouts(self):
        with self.assertRaises(ValueError):
            Client.from_url('clickhouse://host?connect_timeout=test')

        c = Client.from_url('clickhouse://host?connect_timeout=1.2')
        self.assertEqual(c.connection.connect_timeout, 1.2)

        c = Client.from_url('clickhouse://host?send_receive_timeout=1.2')
        self.assertEqual(c.connection.send_receive_timeout, 1.2)

        c = Client.from_url('clickhouse://host?sync_request_timeout=1.2')
        self.assertEqual(c.connection.sync_request_timeout, 1.2)

    def test_compress_block_size(self):
        with self.assertRaises(ValueError):
            Client.from_url('clickhouse://host?compress_block_size=test')

        c = Client.from_url('clickhouse://host?compress_block_size=100500')
        # compression is not set
        self.assertIsNone(c.connection.compress_block_size)

        c = Client.from_url(
            'clickhouse://host?'
            'compress_block_size=100500&'
            'compression=1'
        )
        self.assertEqual(c.connection.compress_block_size, 100500)

    def test_settings(self):
        c = Client.from_url(
            'clickhouse://host?'
            'send_logs_level=trace&'
            'max_block_size=123'
        )
        self.assertEqual(c.settings, {
            'send_logs_level': 'trace',
            'max_block_size': '123'
        })

    def test_ssl(self):
        c = Client.from_url(
            'clickhouses://host?'
            'verify=false&'
            'ssl_version=PROTOCOL_SSLv23&'
            'ca_certs=/tmp/certs&'
            'ciphers=HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5'
        )
        self.assertEqual(c.connection.ssl_options, {
            'ssl_version': ssl.PROTOCOL_SSLv23,
            'ca_certs': '/tmp/certs',
            'ciphers': 'HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5'
        })

    def test_ssl_key_cert(self):
        base_url = (
            'clickhouses://host?'
            'verify=true&'
            'ssl_version=PROTOCOL_SSLv23&'
            'ca_certs=/tmp/certs&'
            'ciphers=HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5&'
        )
        base_expected = {
            'ssl_version': ssl.PROTOCOL_SSLv23,
            'ca_certs': '/tmp/certs',
            'ciphers': 'HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5'
        }

        c = Client.from_url(
            base_url +
            'keyfile=/tmp/client.key&'
            'certfile=/tmp/client.cert'
        )
        expected = base_expected.copy()
        expected.update({
            'keyfile': '/tmp/client.key',
            'certfile': '/tmp/client.cert'
        })
        self.assertEqual(c.connection.ssl_options, expected)

        c = Client.from_url(
            base_url +
            'certfile=/tmp/client.cert'
        )
        expected = base_expected.copy()
        expected.update({
            'certfile': '/tmp/client.cert'
        })
        self.assertEqual(c.connection.ssl_options, expected)

    def test_alt_hosts(self):
        c = Client.from_url('clickhouse://host?alt_hosts=host2:1234')
        self.assertHostsEqual(c, [('host', 9000), ('host2', 1234)])

        c = Client.from_url('clickhouse://host?alt_hosts=host2')
        self.assertHostsEqual(c, [('host', 9000), ('host2', 9000)])

    def test_parameters_cast(self):
        c = Client.from_url('clickhouse://host?insert_block_size=123')
        self.assertEqual(
            c.connection.context.client_settings['insert_block_size'], 123
        )

    def test_settings_is_important(self):
        c = Client.from_url('clickhouse://host?settings_is_important=1')
        self.assertEqual(c.connection.settings_is_important, True)

        with self.assertRaises(ValueError):
            c = Client.from_url('clickhouse://host?settings_is_important=2')
            self.assertEqual(c.connection.settings_is_important, True)

        c = Client.from_url('clickhouse://host?settings_is_important=0')
        self.assertEqual(c.connection.settings_is_important, False)

    @check_numpy
    def test_use_numpy(self):
        c = Client.from_url('clickhouse://host?use_numpy=true')
        self.assertTrue(c.connection.context.client_settings['use_numpy'])

    def test_opentelemetry(self):
        c = Client.from_url(
            'clickhouse://host?opentelemetry_traceparent='
            '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00'
        )
        self.assertEqual(
            c.connection.context.client_settings['opentelemetry_traceparent'],
            '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00'
        )
        self.assertEqual(
            c.connection.context.client_settings['opentelemetry_tracestate'],
            ''
        )

        c = Client.from_url(
            'clickhouse://host?opentelemetry_traceparent='
            '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00&'
            'opentelemetry_tracestate=state'
        )
        self.assertEqual(
            c.connection.context.client_settings['opentelemetry_traceparent'],
            '00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00'
        )
        self.assertEqual(
            c.connection.context.client_settings['opentelemetry_tracestate'],
            'state'
        )

    def test_quota_key(self):
        c = Client.from_url('clickhouse://host?quota_key=myquota')
        self.assertEqual(
            c.connection.context.client_settings['quota_key'], 'myquota'
        )

        c = Client.from_url('clickhouse://host')
        self.assertEqual(
            c.connection.context.client_settings['quota_key'], ''
        )
