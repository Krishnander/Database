import json
import os
import requests
import threading

class KeyValueStore:
    def __init__(self, db_file='database.json', wal_max_entries=100, role='primary', replicas=None):
        self._db_file = db_file
        self._wal_file = db_file + '.wal'
        self._wal_max_entries = wal_max_entries
        self._wal_entries = 0
        self._role = role
        self._replicas = replicas if replicas is not None else []
        self._data = self._load_from_disk()
        self._locks = {}
        self._locks_lock = threading.RLock() # To protect access to the _locks dictionary

    def _get_lock(self, key):
        with self._locks_lock:
            if key not in self._locks:
                self._locks[key] = threading.RLock()
            return self._locks[key]

    def add_replica(self, replica_url):
        if replica_url not in self._replicas:
            self._replicas.append(replica_url)

    def _load_from_disk(self):
        if os.path.exists(self._db_file):
            with open(self._db_file, 'r') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = {}
        else:
            data = {}

        # Replay WAL
        if os.path.exists(self._wal_file):
            with open(self._wal_file, 'r') as f:
                for line in f:
                    try:
                        self._wal_entries += 1
                        parts = json.loads(line)
                        op = parts['op']
                        key = parts['key']
                        if op == 'set':
                            value = parts['value']
                            data[key] = value
                        elif op == 'delete':
                            if key in data:
                                del data[key]
                    except (json.JSONDecodeError, KeyError):
                        # Skip corrupted lines
                        continue
        return data

    def _save_to_disk(self):
        with open(self._db_file, 'w') as f:
            json.dump(self._data, f, indent=4)

    def _compact(self):
        self._save_to_disk()
        if os.path.exists(self._wal_file):
            os.remove(self._wal_file)
        self._wal_entries = 0

    def _append_to_wal(self, op, key, value=None):
        with open(self._wal_file, 'a') as f:
            log_entry = {'op': op, 'key': key}
            if value is not None:
                log_entry['value'] = value
            f.write(json.dumps(log_entry) + '\n')

        self._wal_entries += 1
        if self._wal_entries >= self._wal_max_entries:
            self._compact()

    def _replicate(self, op, key, value=None):
        for replica_url in self._replicas:
            try:
                payload = {'op': op, 'key': key}
                if value is not None:
                    payload['value'] = value

                requests.post(f"{replica_url}/replicate", json=payload, timeout=0.5)
            except requests.RequestException as e:
                print(f"Error replicating to {replica_url}: {e}")

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, replicated=False):
        lock = self._get_lock(key)
        with lock:
            self._data[key] = value
            if self._role == 'primary':
                self._append_to_wal('set', key, value)
                self._replicate('set', key, value)
            elif replicated:
                self._append_to_wal('set', key, value)

    def atomic_update(self, key, update_function):
        lock = self._get_lock(key)
        with lock:
            current_value = self.get(key)
            new_value = update_function(current_value)
            self.set(key, new_value)

    def delete(self, key, replicated=False):
        lock = self._get_lock(key)
        with lock:
            if key in self._data:
                del self._data[key]
                if self._role == 'primary':
                    self._append_to_wal('delete', key)
                    self._replicate('delete', key)
                elif replicated:
                    self._append_to_wal('delete', key)
                return True
            return False
