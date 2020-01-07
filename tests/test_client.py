import ssl
from unittest import TestCase

from clickhouse_driver import Client
from clickhouse_driver.compression.lz4 import Compressor as LZ4Compressor
from clickhouse_driver.compression.lz4hc import Compressor as LZHC4Compressor
from clickhouse_driver.compression.zstd import Compressor as ZSTDCompressor
from clickhouse_driver.protocol import Compression


class ClientFromUrlTestCase(TestCase):

    def test_simple(self):
        c = Client.from_url('clickhouse://host')

        assert c.connection.hosts == [('host', 9000)]
        assert c.connection.database == 'default'

        c = Client.from_url('clickhouse://host/db')

        assert c.connection.hosts == [('host', 9000)]
        assert c.connection.database == 'db'

    def test_credentials(self):
        c = Client.from_url('clickhouse://host/db')

        assert c.connection.user == 'default'
        assert c.connection.password == ''

        c = Client.from_url('clickhouse://admin:secure@host/db')

        assert c.connection.user == 'admin'
        assert c.connection.password == 'secure'

        c = Client.from_url('clickhouse://user:@host/db')

        assert c.connection.user == 'user'
        assert c.connection.password == ''

    def test_schema(self):
        c = Client.from_url('clickhouse://host')
        assert c.connection.secure_socket is False

        c = Client.from_url('clickhouses://host')
        assert c.connection.secure_socket is True

        c = Client.from_url('test://host')
        assert c.connection.secure_socket is False

    def test_port(self):
        c = Client.from_url('clickhouse://host')
        assert c.connection.hosts == [('host', 9000)]

        c = Client.from_url('clickhouses://host')
        assert c.connection.hosts == [('host', 9440)]

        c = Client.from_url('clickhouses://host:1234')
        assert c.connection.hosts == [('host', 1234)]

    def test_secure(self):
        c = Client.from_url('clickhouse://host?secure=n')
        assert c.connection.hosts == [('host', 9000)]
        assert c.connection.secure_socket is False

        c = Client.from_url('clickhouse://host?secure=y')
        assert c.connection.hosts == [('host', 9440)]
        assert c.connection.secure_socket is True

        c = Client.from_url('clickhouse://host:1234?secure=y')
        assert c.connection.hosts == [('host', 1234)]
        assert c.connection.secure_socket is True

        with self.assertRaises(ValueError):
            Client.from_url('clickhouse://host:1234?secure=nonono')

    def test_compression(self):
        c = Client.from_url('clickhouse://host?compression=n')
        assert c.connection.compression == Compression.DISABLED
        assert c.connection.compressor_cls is None

        c = Client.from_url('clickhouse://host?compression=y')
        assert c.connection.compression == Compression.ENABLED
        assert c.connection.compressor_cls is LZ4Compressor

        c = Client.from_url('clickhouse://host?compression=lz4')
        assert c.connection.compression == Compression.ENABLED
        assert c.connection.compressor_cls is LZ4Compressor

        c = Client.from_url('clickhouse://host?compression=lz4hc')
        assert c.connection.compression == Compression.ENABLED
        assert c.connection.compressor_cls is LZHC4Compressor

        c = Client.from_url('clickhouse://host?compression=zstd')
        assert c.connection.compression == Compression.ENABLED
        assert c.connection.compressor_cls is ZSTDCompressor

        with self.assertRaises(ValueError):
            Client.from_url('clickhouse://host:1234?compression=custom')

    def test_client_name(self):
        c = Client.from_url('clickhouse://host?client_name=native')
        assert c.connection.client_name == 'ClickHouse native'

    def test_timeouts(self):
        with self.assertRaises(ValueError):
            Client.from_url('clickhouse://host?connect_timeout=test')

        c = Client.from_url('clickhouse://host?connect_timeout=1.2')
        assert c.connection.connect_timeout == 1.2

        c = Client.from_url('clickhouse://host?send_receive_timeout=1.2')
        assert c.connection.send_receive_timeout == 1.2

        c = Client.from_url('clickhouse://host?sync_request_timeout=1.2')
        assert c.connection.sync_request_timeout == 1.2

    def test_compress_block_size(self):
        with self.assertRaises(ValueError):
            Client.from_url('clickhouse://host?compress_block_size=test')

        c = Client.from_url('clickhouse://host?compress_block_size=100500')
        # compression is not set
        assert c.connection.compress_block_size is None

        c = Client.from_url(
            'clickhouse://host?'
            'compress_block_size=100500&'
            'compression=1'
        )
        assert c.connection.compress_block_size == 100500

    def test_settings(self):
        c = Client.from_url(
            'clickhouse://host?'
            'send_logs_level=trace&'
            'max_block_size=123'
        )
        assert c.settings == {
            'send_logs_level': 'trace',
            'max_block_size': '123'
        }

    def test_ssl(self):
        c = Client.from_url(
            'clickhouses://host?'
            'verify=false&'
            'ssl_version=PROTOCOL_SSLv23&'
            'ca_certs=/tmp/certs&'
            'ciphers=HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5'
        )
        assert c.connection.ssl_options == {
            'ssl_version': ssl.PROTOCOL_SSLv23,
            'ca_certs': '/tmp/certs',
            'ciphers': 'HIGH:-aNULL:-eNULL:-PSK:RC4-SHA:RC4-MD5'
        }

    def test_alt_hosts(self):
        c = Client.from_url('clickhouse://host?alt_hosts=host2:1234')
        assert c.connection.hosts == [('host', 9000), ('host2', 1234)]

        c = Client.from_url('clickhouse://host?alt_hosts=host2')
        assert c.connection.hosts == [('host', 9000), ('host2', 9000)]

    def test_parameters_cast(self):
        c = Client.from_url('clickhouse://host?insert_block_size=123')
        assert c.connection.context.client_settings['insert_block_size'] == 123
