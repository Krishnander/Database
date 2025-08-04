import os
import unittest
import json
from unittest.mock import patch
from app import app
from database import KeyValueStore

class KeyValueStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_database.json'
        self.wal_file = self.db_file + '.wal'
        self.db = KeyValueStore(db_file=self.db_file, wal_max_entries=3)

    def tearDown(self):
        if os.path.exists(self.db_.file):
            os.remove(self.db_file)
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)

    def test_add_replica(self):
        self.db.add_replica('http://localhost:5001')
        self.assertIn('http://localhost:5001', self.db._replicas)

    def test_set_get(self):
        self.db.set('name', 'Jules')
        self.assertEqual(self.db.get('name'), 'Jules')

    def test_delete(self):
        self.db.set('city', 'Paris')
        self.assertTrue(self.db.delete('city'))
        self.assertIsNone(self.db.get('city'))
        self.assertFalse(self.db.delete('non_existent_key'))

    def test_wal_recovery(self):
        self.db.set('country', 'France')
        self.db.set('capital', 'Paris')
        self.db.delete('country')

        new_db = KeyValueStore(db_file=self.db_file)
        self.assertEqual(new_db.get('capital'), 'Paris')
        self.assertIsNone(new_db.get('country'))

    def test_compaction(self):
        self.db.set('a', '1')
        self.db.set('b', '2')
        self.db.set('c', '3')

        self.assertTrue(os.path.exists(self.db_file))
        self.assertFalse(os.path.exists(self.wal_file))

        new_db = KeyValueStore(db_file=self.db_file)
        self.assertEqual(new_db.get('a'), '1')
        self.assertEqual(new_db.get('b'), '2')
        self.assertEqual(new_db.get('c'), '3')


class FlaskApiTestCase(unittest.TestCase):
    def setUp(self):
        self.primary_db_file = 'test_primary_db.json'
        self.replica_db_file = 'test_replica_db.json'
        self.primary_wal_file = self.primary_db_file + '.wal'
        self.replica_wal_file = self.replica_db_file + '.wal'

        self.primary_db = KeyValueStore(db_file=self.primary_db_file, role='primary')
        self.replica_db = KeyValueStore(db_file=self.replica_db_file, role='replica')

        app.config['TESTING'] = True
        self.app = app.test_client()

    def tearDown(self):
        for f in [self.primary_db_file, self.replica_db_file, self.primary_wal_file, self.replica_wal_file]:
            if os.path.exists(f):
                os.remove(f)

    def test_register_replica_api(self):
        import app as flask_app
        flask_app.db = self.primary_db

        response = self.app.post('/register_replica',
                                 data=json.dumps({'replica_url': 'http://localhost:5001'}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertIn('http://localhost:5001', self.primary_db._replicas)

    def test_replicate_api(self):
        import app as flask_app
        flask_app.db = self.replica_db

        response = self.app.post('/replicate',
                                 data=json.dumps({'op': 'set', 'key': 'color', 'value': 'blue'}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.replica_db.get('color'), 'blue')

    @patch('requests.post')
    def test_replication_on_set(self, mock_post):
        import app as flask_app
        flask_app.db = self.primary_db
        self.primary_db.add_replica('http://localhost:5001')

        response = self.app.post('/set',
                                 data=json.dumps({'key': 'animal', 'value': 'cat'}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 200)
        mock_post.assert_called_once_with(
            'http://localhost:5001/replicate',
            json={'op': 'set', 'key': 'animal', 'value': 'cat'},
            timeout=0.5
        )

    def test_write_on_replica_rejected(self):
        import app as flask_app
        flask_app.db = self.replica_db

        response = self.app.post('/set',
                                 data=json.dumps({'key': 'food', 'value': 'pizza'}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 403)


if __name__ == '__main__':
    unittest.main()
