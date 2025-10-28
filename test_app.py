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
        self.db = KeyValueStore(db_file=self.db_file, wal_max_entries=3, testing=True)

    def tearDown(self):
        if os.path.exists(self.db._storage._db_file):
            os.remove(self.db._storage._db_file)
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

        new_db = KeyValueStore(db_file=self.db_file, testing=True)
        self.assertEqual(new_db.get('capital'), 'Paris')
        self.assertIsNone(new_db.get('country'))

    def test_compaction(self):
        self.db.set('a', '1')
        self.db.set('b', '2')
        self.db.set('c', '3')

        self.assertTrue(os.path.exists(self.db._storage._db_file))
        self.assertFalse(os.path.exists(self.wal_file))

        new_db = KeyValueStore(db_file=self.db_file, testing=True)
        self.assertEqual(new_db.get('a'), '1')
        self.assertEqual(new_db.get('b'), '2')
        self.assertEqual(new_db.get('c'), '3')


class FlaskApiTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_api_db.json'
        self.wal_file = self.db_file + '.wal'
        self.db = KeyValueStore(db_file=self.db_file, testing=True)

        self.patcher = patch('app.db', self.db)
        self.patcher.start()

        app.config['TESTING'] = True
        self.app = app.test_client()
        import app as flask_app
        flask_app.VALID_API_KEYS = ['test-key']

    def tearDown(self):
        self.patcher.stop()
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)

    def test_set_get_delete_api(self):
        # Test setting a value
        response = self.app.post('/set',
                                 headers={'Authorization': 'test-key'},
                                 data=json.dumps({'key': 'drink', 'value': 'water'}),
                                 content_type='application/json')
        self.assertEqual(response.status_code, 200)

        # Test getting a value
        response = self.app.get('/get/drink', headers={'Authorization': 'test-key'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['value'], 'water')

        # Test deleting a value
        response = self.app.delete('/delete/drink', headers={'Authorization': 'test-key'})
        self.assertEqual(response.status_code, 200)

        # Verify value is deleted
        response = self.app.get('/get/drink', headers={'Authorization': 'test-key'})
        self.assertEqual(response.status_code, 404)


class TransactionTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_transaction_db.json'
        self.wal_file = self.db_file + '.wal'
        self.db = KeyValueStore(db_file=self.db_file, testing=True)

    def tearDown(self):
        if os.path.exists(self.db._storage._db_file):
            os.remove(self.db._storage._db_file)
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)

    def test_commit(self):
        transaction_id = self.db.begin()
        self.db.add_op(transaction_id, 'set', 'a', 1)
        self.db.add_op(transaction_id, 'set', 'b', 2)
        self.assertTrue(self.db.commit(transaction_id))
        self.assertEqual(self.db.get('a'), 1)
        self.assertEqual(self.db.get('b'), 2)

    def test_rollback(self):
        self.db.set('a', 0)
        transaction_id = self.db.begin()
        self.db.add_op(transaction_id, 'set', 'a', 1)
        self.assertTrue(self.db.rollback(transaction_id))
        self.assertEqual(self.db.get('a'), 0)

    def test_atomic_commit(self):
        self.db.set('a', 0)
        self.db.set('b', 0)
        transaction_id = self.db.begin()
        self.db.add_op(transaction_id, 'set', 'a', 1)
        self.db.add_op(transaction_id, 'set', 'b', 2)

        with patch.object(self.db, 'set', side_effect=Exception('Simulated failure')):
            self.assertFalse(self.db.commit(transaction_id))

        self.assertEqual(self.db.get('a'), 0)
        self.assertEqual(self.db.get('b'), 0)


class IndexTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_index_db.json'
        self.wal_file = self.db_file + '.wal'
        self.db = KeyValueStore(db_file=self.db_file, testing=True)

    def tearDown(self):
        if os.path.exists(self.db._storage._db_file):
            os.remove(self.db._storage._db_file)
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)

    def test_create_index(self):
        self.db.set('user1', {'name': 'Alice', 'age': 30})
        self.db.create_index('age_index', 'age')
        self.assertIn('age_index', self.db._indexes)
        self.assertEqual(self.db._indexes['age_index']['index'][30], ['user1'])

    def test_update_index_on_set(self):
        self.db.create_index('city_index', 'city')
        self.db.set('user1', {'name': 'Alice', 'city': 'New York'})
        self.db.set('user2', {'name': 'Bob', 'city': 'London'})
        self.db.set('user3', {'name': 'Charlie', 'city': 'New York'})

        self.assertEqual(self.db._indexes['city_index']['index']['New York'], ['user1', 'user3'])
        self.assertEqual(self.db._indexes['city_index']['index']['London'], ['user2'])

    def test_update_index_on_delete(self):
        self.db.create_index('city_index', 'city')
        self.db.set('user1', {'name': 'Alice', 'city': 'New York'})
        self.db.set('user2', {'name': 'Bob', 'city': 'London'})
        self.db.delete('user1')

        self.assertNotIn('New York', self.db._indexes['city_index']['index'])

    def test_query(self):
        self.db.create_index('age_index', 'age')
        self.db.set('user1', {'name': 'Alice', 'age': 30})
        self.db.set('user2', {'name': 'Bob', 'age': 40})
        self.db.set('user3', {'name': 'Charlie', 'age': 30})

        results = self.db.query('age_index', 30)
        self.assertEqual(len(results), 2)
        self.assertIn('user1', results)
        self.assertIn('user3', results)


class SecurityTestCase(unittest.TestCase):
    def setUp(self):
        # This test case doesn't need a real db file, but we need to patch the db instance
        self.db = KeyValueStore(db_file='test_security_db.json', testing=True)
        self.patcher = patch('app.db', self.db)
        self.patcher.start()

        self.app = app.test_client()
        app.config['TESTING'] = True
        # Set a valid API key for testing
        import app as flask_app
        flask_app.VALID_API_KEYS = ['test-key']

    def tearDown(self):
        self.patcher.stop()

    def test_missing_api_key(self):
        response = self.app.get('/get/some_key')
        self.assertEqual(response.status_code, 401)

    def test_invalid_api_key(self):
        response = self.app.get('/get/some_key', headers={'Authorization': 'invalid-key'})
        self.assertEqual(response.status_code, 401)

    def test_valid_api_key(self):
        # We expect a 404 here because the key doesn't exist, but a 401
        # would indicate an authentication failure.
        response = self.app.get('/get/some_key', headers={'Authorization': 'test-key'})
        self.assertEqual(response.status_code, 404)


import threading

class ConcurrencyTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = 'test_concurrency_db.json'
        self.wal_file = self.db_file + '.wal'
        self.db = KeyValueStore(db_file=self.db_file, testing=True)
        self.db.set('counter', 0)

    def tearDown(self):
        if os.path.exists(self.db._storage._db_file):
            os.remove(self.db._storage._db_file)
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)

    def worker(self, num_iterations):
        for _ in range(num_iterations):
            self.db.atomic_update('counter', lambda value: value + 1)

    def test_concurrent_writes(self):
        num_threads = 10
        num_iterations_per_thread = 100

        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=self.worker, args=(num_iterations_per_thread,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        expected_value = num_threads * num_iterations_per_thread
        self.assertEqual(self.db.get('counter'), expected_value)


if __name__ == '__main__':
    unittest.main()
