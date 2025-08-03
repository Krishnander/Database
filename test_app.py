import os
import unittest
import json
from app import app
from database import KeyValueStore

class KeyValueStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_database.json'
        self.db = KeyValueStore(db_file=self.db_file)

    def tearDown(self):
        if os.path.exists(self.db_file):
            os.remove(self.db_file)

    def test_set_get(self):
        self.db.set('name', 'Jules')
        self.assertEqual(self.db.get('name'), 'Jules')

    def test_delete(self):
        self.db.set('city', 'Paris')
        self.assertTrue(self.db.delete('city'))
        self.assertIsNone(self.db.get('city'))
        self.assertFalse(self.db.delete('non_existent_key'))

    def test_persistence(self):
        self.db.set('country', 'France')
        # Create a new instance to see if it loads from the file
        new_db = KeyValueStore(db_file=self.db_file)
        self.assertEqual(new_db.get('country'), 'France')

class FlaskApiTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_api_database.json'
        app.config['TESTING'] = True
        # In a real app, you would use a factory or dependency injection
        # to ensure the app uses a test-specific database instance.
        # For this simple case, we'll create a new KeyValueStore instance
        # for testing and assign it to a new test client.
        self.db = KeyValueStore(db_file=self.db_file)
        self.app = app.test_client()
        # This is a bit of a hack. A better approach would be to use Flask's
        # application context to manage the database connection.
        import app as flask_app
        flask_app.db = self.db


    def tearDown(self):
        if os.path.exists(self.db_file):
            os.remove(self.db_file)

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
