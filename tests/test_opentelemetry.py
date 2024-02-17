from tests.testcase import BaseTestCase
from tests.util import capture_logging


class OpenTelemetryTestCase(BaseTestCase):
    required_server_version = (20, 11, 2)

    def test_server_logs(self):
        tracestate = 'tracestate'
        traceparent = '00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'

        settings = {
            'opentelemetry_tracestate': tracestate,
            'opentelemetry_traceparent': traceparent

        }
        with self.created_client(settings=settings) as client:
            with capture_logging('clickhouse_driver.log', 'INFO') as buffer:
                settings = {'send_logs_level': 'trace'}
                query = 'SELECT 1'
                client.execute(query, settings=settings)
                value = buffer.getvalue()
                self.assertIn('OpenTelemetry', value)

                # ClickHouse 22.2+ use big-endian:
                # https://github.com/ClickHouse/ClickHouse/pull/33723
                if self.server_version >= (22, 2):
                    tp = '8448eb211c80319c1af7651916cd43dd'
                else:
                    tp = '1af7651916cd43dd8448eb211c80319c'
                self.assertIn(tp, value)

    def test_no_tracestate(self):
        traceparent = '00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'

        settings = {
            'opentelemetry_traceparent': traceparent

        }
        with self.created_client(settings=settings) as client:
            with capture_logging('clickhouse_driver.log', 'INFO') as buffer:
                settings = {'send_logs_level': 'trace'}
                query = 'SELECT 1'
                client.execute(query, settings=settings)
                value = buffer.getvalue()
                self.assertIn('OpenTelemetry', value)
                # ClickHouse 22.2+ use big-endian:
                # https://github.com/ClickHouse/ClickHouse/pull/33723
                if self.server_version >= (22, 2):
                    tp = '8448eb211c80319c1af7651916cd43dd'
                else:
                    tp = '1af7651916cd43dd8448eb211c80319c'
                self.assertIn(tp, value)

    def test_bad_traceparent(self):
        settings = {'opentelemetry_traceparent': 'bad'}
        with self.created_client(settings=settings) as client:
            with self.assertRaises(ValueError) as e:
                client.execute('SELECT 1')

            self.assertEqual(
                str(e.exception),
                'unexpected length 3, expected 55'
            )

        traceparent = '00-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-yyyyyyyyyyyyyyyy-01'
        settings = {'opentelemetry_traceparent': traceparent}
        with self.created_client(settings=settings) as client:
            with self.assertRaises(ValueError) as e:
                client.execute('SELECT 1')

            self.assertEqual(
                str(e.exception),
                'Malformed traceparant header: {}'.format(traceparent)
            )

    def test_bad_traceparent_version(self):
        settings = {
            'opentelemetry_traceparent':
                '01-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01'
        }
        with self.created_client(settings=settings) as client:
            with self.assertRaises(ValueError) as e:
                client.execute('SELECT 1')

            self.assertEqual(
                str(e.exception),
                'unexpected version 01, expected 00'
            )
