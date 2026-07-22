from unittest import TestCase

from clickhouse_driver._cityhash.cityhash import CityHash128


class CityHash128TestCase(TestCase):
    # Golden values for the vendored CityHash 1.0.2 algorithm. ClickHouse
    # hashes compressed blocks with this exact version, so these must never
    # change -- a mismatch means we broke wire compatibility with the server.
    GOLDEN = {
        b'': 82332263323914296566372529678324145705,
        b'a': 279725777567359144512642526446120103158,
        b'abc': 191491615738686005514802259311635598718,
        b'hello world': 167458446662975529045819099149800539344,
        b'x' * 63: 315746516683536713704198863747634701618,
        b'x' * 64: 201844484092394838928428687108119794129,
        b'x' * 65: 245755329935730809217271693433052602484,
        bytes(range(256)): 67103632049168710853453900228853218707,
    }

    def test_golden_vectors(self):
        for data, expected in self.GOLDEN.items():
            self.assertEqual(CityHash128(data), expected)

    def test_result_is_128_bit_int(self):
        value = CityHash128(b'abc')
        self.assertIsInstance(value, int)
        self.assertTrue(0 <= value < (1 << 128))

    def test_str_matches_utf8_bytes(self):
        self.assertEqual(CityHash128('abracadabra'),
                         CityHash128('abracadabra'.encode('utf-8')))

    def test_empty_str_matches_empty_bytes(self):
        self.assertEqual(CityHash128(''), CityHash128(b''))

    def test_buffer_matches_bytes(self):
        self.assertEqual(CityHash128(memoryview(b'abc')), CityHash128(b'abc'))

    def test_rejects_unsupported_type(self):
        with self.assertRaises(TypeError):
            CityHash128(123)
