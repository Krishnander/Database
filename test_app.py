import os
import unittest
import json
from app import app
from database import KeyValueStore

class KeyValueStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_database.json'
        self.wal_file = self.db_file + '.wal'
        self.db = KeyValueStore(db_file=self.db_file, wal_max_entries=3)

    def tearDown(self):
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)

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

        # Create a new instance to trigger recovery
        new_db = KeyValueStore(db_file=self.db_file)
        self.assertEqual(new_db.get('capital'), 'Paris')
        self.assertIsNone(new_db.get('country'))

    def test_compaction(self):
        self.db.set('a', '1')
        self.db.set('b', '2')
        self.db.set('c', '3') # This should trigger compaction

        self.assertTrue(os.path.exists(self.db_file))
        self.assertFalse(os.path.exists(self.wal_file))

        # Verify data is correct after compaction
        new_db = KeyValueStore(db_file=self.db_file)
        self.assertEqual(new_db.get('a'), '1')
        self.assertEqual(new_db.get('b'), '2')
        self.assertEqual(new_db.get('c'), '3')


class FlaskApiTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_api_database.json'
        self.wal_file = self.db_file + '.wal'
        app.config['TESTING'] = True
        self.db = KeyValueStore(db_file=self.db_file)
        import app as flask_app
        flask_app.db = self.db
        self.app = app.test_client()


    def tearDown(self):
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)

    def test_set_api(self):
        response = self.app.post('/set',
                                 data=json.dumps({'key': 'color', 'value': 'blue'}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['message'], 'Key "color" set successfully.')
        self.assertEqual(self.db.get('color'), 'blue')

    def test_get_api(self):
        self.db.set('animal', 'cat')
        response = self.app.get('/get/animal')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['animal'], 'cat')

    def test_get_non_existent_key_api(self):
        response = self.app.get('/get/non_existent_key')
        self.assertEqual(response.status_code, 404)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Key not found')

    def test_delete_api(self):
        self.db.set('fruit', 'apple')
        response = self.app.delete('/delete/fruit')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data['message'], 'Key "fruit" deleted successfully.')
        self.assertIsNone(self.db.get('fruit'))

    def test_delete_non_existent_key_api(self):
        response = self.app.delete('/delete/non_existent_key')
        self.assertEqual(response.status_code, 404)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Key not found')

    def test_set_invalid_request_api(self):
        response = self.app.post('/set',
                                 data=json.dumps({'key': 'food'}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertEqual(data['error'], 'Invalid request. "key" and "value" are required.')

if __name__ == '__main__':
    unittest.main()
