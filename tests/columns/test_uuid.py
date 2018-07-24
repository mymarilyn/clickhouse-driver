from uuid import UUID
from tests.testcase import BaseTestCase
from clickhouse_driver import errors


class UUIDTestCase(BaseTestCase):
    def test_simple(self):
        with self.create_table('a UUID'):
            data = [
                (UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'), ),
                ('2efcead4-ff55-4db5-bdb4-6b36a308d8e0', )
            ]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted, (
                'c0fcbba9-0752-44ed-a5d6-4dfb4342b89d\n'
                '2efcead4-ff55-4db5-bdb4-6b36a308d8e0\n'
            ))
            inserted = self.client.execute(query)
            self.assertEqual(inserted, [
                (UUID('c0fcbba9-0752-44ed-a5d6-4dfb4342b89d'), ),
                (UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0'), )
            ])

    def test_type_mismatch(self):
        data = [(62457709573696417404743346296141175008, )]
        with self.create_table('a UUID'):
            with self.assertRaises(errors.TypeMismatchError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data, types_check=True
                )
            with self.assertRaises(AttributeError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_bad_uuid(self):
        data = [('a', )]
        with self.create_table('a UUID'):
            with self.assertRaises(errors.CannotParseUuidError):
                self.client.execute(
                    'INSERT INTO test (a) VALUES', data
                )

    def test_nullable(self):
        with self.create_table('a Nullable(UUID)'):
            data = [(UUID('2efcead4-ff55-4db5-bdb4-6b36a308d8e0'), ), (None, )]
            self.client.execute(
                'INSERT INTO test (a) VALUES', data
            )

            query = 'SELECT * FROM test'
            inserted = self.emit_cli(query)
            self.assertEqual(inserted,
                             '2efcead4-ff55-4db5-bdb4-6b36a308d8e0\n\\N\n')

            inserted = self.client.execute(query)
            self.assertEqual(inserted, data)
