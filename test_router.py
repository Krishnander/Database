import unittest
from unittest.mock import patch, MagicMock
from router import app
import os
from uhashring import HashRing

class RouterTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        app.config['TESTING'] = True

        # Configure the shards for the test
        self.shards = ['http://shard1', 'http://shard2']
        os.environ['SHARDS'] = ','.join(self.shards)

        # Create a new ring for testing
        self.ring = HashRing(nodes=self.shards)

    def get_shard(self, key):
        return self.ring.get_node(key)

    @patch('router.requests.get')
    def test_get_key(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"mykey": "myvalue"}'
        mock_get.return_value = mock_response

        with patch('router.get_shard', self.get_shard):
            response = self.app.get('/get/mykey')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, b'{"mykey": "myvalue"}')

            shard_url = self.get_shard('mykey')
            mock_get.assert_called_once_with(f"{shard_url}/get/mykey")

    @patch('router.requests.post')
    def test_set_key(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"message": "Key set"}'
        mock_post.return_value = mock_response

        with patch('router.get_shard', self.get_shard):
            response = self.app.post('/set', json={'key': 'mykey', 'value': 'myvalue'})
            self.assertEqual(response.status_code, 200)

            shard_url = self.get_shard('mykey')
            mock_post.assert_called_once_with(f"{shard_url}/set", json={'key': 'mykey', 'value': 'myvalue'})

    @patch('router.requests.delete')
    def test_delete_key(self, mock_delete):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"message": "Key deleted"}'
        mock_delete.return_value = mock_response

        with patch('router.get_shard', self.get_shard):
            response = self.app.delete('/delete/mykey')
            self.assertEqual(response.status_code, 200)

            shard_url = self.get_shard('mykey')
            mock_delete.assert_called_once_with(f"{shard_url}/delete/mykey")


if __name__ == '__main__':
    unittest.main()
