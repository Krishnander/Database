import unittest
import time
import requests
import subprocess
import os

class TestHighAvailability(unittest.TestCase):
    def setUp(self):
        self.node_processes = []
        self.router_process = None

        # Create a dedicated environment for the subprocesses with the API key
        env = os.environ.copy()
        env['API_KEYS'] = 'ha-test-key'

        # Start 3 nodes
        for port in [5000, 5001, 5002]:
            peers = "http://localhost:5000,http://localhost:5001,http://localhost:5002"
            proc = subprocess.Popen(
                ['python3', 'app.py', '--port', str(port), '--peers', peers],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )
            self.node_processes.append(proc)

        # Start the router
        shards = "http://localhost:5000;http://localhost:5001;http://localhost:5002"
        self.router_process = subprocess.Popen(
            ['python3', 'router.py', '--port', '8000', '--shards', shards],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)  # Give the servers time to start

    def tearDown(self):
        if self.router_process:
            self.router_process.terminate()
            stdout, stderr = self.router_process.communicate()
            print("Router stdout:")
            print(stdout.decode())
            print("Router stderr:")
            print(stderr.decode())
        for proc in self.node_processes:
            proc.terminate()
            stdout, stderr = proc.communicate()
            print(f"Node {proc.pid} stdout:")
            print(stdout.decode())
            print(f"Node {proc.pid} stderr:")
            print(stderr.decode())

    def test_leader_forwarding(self):
        # In our current setup, node 5000 is manually set as the leader.
        # Let's send a set request and see if it's successful.
        response = requests.post("http://localhost:8000/set", json={'key': 'name', 'value': 'Jules'}, headers={'Authorization': 'ha-test-key'})
        self.assertEqual(response.status_code, 200)

        # Now, let's verify that the value was set on the leader (node 5000)
        response = requests.get("http://localhost:5000/get/name", headers={'Authorization': 'ha-test-key'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'value': 'Jules'})

    def test_leader_failover(self):
        # Initial leader is 5000. Let's kill it.
        self.node_processes[0].terminate()
        self.node_processes[0].wait()

        # Manually set node 5001 as the new leader
        requests.post("http://localhost:5001/force_leader")

        # The router should now forward requests to node 5001.
        # We need to clear the leader cache in the router.
        # Since we can't do that directly, we'll just wait for it to expire.
        # In a real implementation, the cache would have a TTL.
        # For this test, we'll just send a request and expect it to fail once, then succeed.

        # This is a bit of a hack. A better solution would be to have a way to invalidate the cache.
        # For now, we will just try a few times.
        for i in range(5):
            response = requests.post("http://localhost:8000/set", json={'key': 'city', 'value': 'Paris'}, headers={'Authorization': 'ha-test-key'})
            if response.status_code == 200:
                break
            time.sleep(0.5)

        self.assertEqual(response.status_code, 200)

        # Verify the value was set on the new leader (node 5001)
        response = requests.get("http://localhost:5001/get/city", headers={'Authorization': 'ha-test-key'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'value': 'Paris'})

if __name__ == '__main__':
    unittest.main()
