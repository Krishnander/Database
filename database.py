import json
import os
import requests
import threading

class Transaction:
    def __init__(self, transaction_id):
        self.id = transaction_id
        self.operations = []
        self.keys = set()

    def add_operation(self, op, key, value=None):
        self.operations.append({'op': op, 'key': key, 'value': value})
        self.keys.add(key)

class KeyValueStore:
    def __init__(self, db_file='database.json', wal_max_entries=100, role='primary', replicas=None):
        self._db_file = db_file
        self._wal_file = db_file + '.wal'
        self._wal_max_entries = wal_max_entries
        self._wal_entries = 0
        self._role = role
        self._replicas = replicas if replicas is not None else []
        self._data = self._load_from_disk()
        self._transactions = {}
        self._next_transaction_id = 0
        self._locks = {}
        self._locks_lock = threading.RLock()

    def begin(self):
        transaction_id = self._next_transaction_id
        self._next_transaction_id += 1
        self._transactions[transaction_id] = Transaction(transaction_id)
        return transaction_id

    def add_op(self, transaction_id, op, key, value=None):
        if transaction_id not in self._transactions:
            raise ValueError("Transaction not found.")
        self._transactions[transaction_id].add_operation(op, key, value)

    def commit(self, transaction_id):
        if transaction_id not in self._transactions:
            raise ValueError("Transaction not found.")

        transaction = self._transactions[transaction_id]

        # Acquire locks for all keys in the transaction
        locks = [self._get_lock(key) for key in sorted(list(transaction.keys))]
        for lock in locks:
            lock.acquire()

        try:
            # Execute operations
            for op in transaction.operations:
                if op['op'] == 'set':
                    self._data[op['key']] = op['value']
                    if self._role == 'primary':
                        self._append_to_wal('set', op['key'], op['value'])
                        self._replicate('set', op['key'], op['value'])
                elif op['op'] == 'delete':
                    if op['key'] in self._data:
                        del self._data[op['key']]
                        if self._role == 'primary':
                            self._append_to_wal('delete', op['key'])
                            self._replicate('delete', op['key'])
        except Exception as e:
            # Rollback changes
            # This is still a simplification. A real implementation would
            # need to restore the original values of the keys.
            print(f"Transaction failed, rolling back: {e}")
            return False
        finally:
            # Release locks
            for lock in locks:
                lock.release()

            del self._transactions[transaction_id]

        return True

    def rollback(self, transaction_id):
        if transaction_id not in self._transactions:
            raise ValueError("Transaction not found.")

        del self._transactions[transaction_id]
        return True

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
        self._data[key] = value
        if self._role == 'primary':
            self._append_to_wal('set', key, value)
            self._replicate('set', key, value)
        elif replicated:
            self._append_to_wal('set', key, value)

    def delete(self, key, replicated=False):
        if key in self._data:
            del self._data[key]
            if self._role == 'primary':
                self._append_to_wal('delete', key)
                self._replicate('delete', key)
            elif replicated:
                self._append_to_wal('delete', key)
            return True
        return False
