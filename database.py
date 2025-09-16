import json
import os
import requests
import threading
from prometheus_client import Counter, Gauge
from abc import ABC, abstractmethod

class StorageEngine(ABC):
    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def set(self, key, value):
        pass

    @abstractmethod
    def delete(self, key):
        pass

    @abstractmethod
    def keys(self):
        pass

    @abstractmethod
    def close(self):
        pass

class JSONStorageEngine(StorageEngine):
    def __init__(self, db_file):
        self._db_file = db_file
        self._data = self._load_from_disk()

    def _load_from_disk(self):
        if os.path.exists(self._db_file):
            with open(self._db_file, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}

    def _save_to_disk(self):
        with open(self._db_file, 'w') as f:
            json.dump(self._data, f, indent=4)

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value

    def delete(self, key):
        if key in self._data:
            del self._data[key]
            return True
        return False

    def keys(self):
        return self._data.keys()

    def close(self):
        self._save_to_disk()


class Transaction:
    def __init__(self, transaction_id):
        self.id = transaction_id
        self.operations = []
        self.keys = set()

    def add_operation(self, op, key, value=None):
        self.operations.append({'op': op, 'key': key, 'value': value})
        self.keys.add(key)

# Metrics
GET_OPS = Counter('db_get_operations_total', 'Total number of get operations.')
SET_OPS = Counter('db_set_operations_total', 'Total number of set operations.')
DELETE_OPS = Counter('db_delete_operations_total', 'Total number of delete operations.')
TRANSACTIONS_BEGUN = Counter('db_transactions_begun_total', 'Total number of transactions begun.')
TRANSACTIONS_COMMITTED = Counter('db_transactions_committed_total', 'Total number of transactions committed.')
TRANSACTIONS_ROLLED_BACK = Counter('db_transactions_rolled_back_total', 'Total number of transactions rolled back.')
REPLICATION_EVENTS = Counter('db_replication_events_total', 'Total number of replication events.')
INDEXES_CREATED = Counter('db_indexes_created_total', 'Total number of indexes created.')
QUERIES = Counter('db_queries_total', 'Total number of queries.')
TOTAL_KEYS = Gauge('db_total_keys', 'Total number of keys in the database.')

class KeyValueStore:
    def __init__(self, db_file='database.json', wal_max_entries=100, role='primary', replicas=None):
        self._storage = JSONStorageEngine(db_file)
        self._wal_file = db_file + '.wal'
        self._wal_max_entries = wal_max_entries
        self._wal_entries = 0
        self._role = role
        self._replicas = replicas if replicas is not None else []
        self._replay_wal()
        TOTAL_KEYS.set(len(list(self._storage.keys())))
        self._transactions = {}
        self._next_transaction_id = 0
        self._locks = {}
        self._locks_lock = threading.RLock()
        self._indexes = {}

    def _replay_wal(self):
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
                            self._storage.set(key, value)
                        elif op == 'delete':
                            self._storage.delete(key)
                    except (json.JSONDecodeError, KeyError):
                        # Skip corrupted lines
                        continue

    def create_index(self, index_name, field):
        INDEXES_CREATED.inc()
        if index_name in self._indexes:
            raise ValueError(f"Index '{index_name}' already exists.")

        self._indexes[index_name] = {'field': field, 'index': {}}

        # Build the index from existing data
        for key in self._storage.keys():
            value = self._storage.get(key)
            if isinstance(value, dict) and field in value:
                field_value = value[field]
                if field_value not in self._indexes[index_name]['index']:
                    self._indexes[index_name]['index'][field_value] = []
                self._indexes[index_name]['index'][field_value].append(key)

    def begin(self):
        TRANSACTIONS_BEGUN.inc()
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

        # Store the original values of the keys
        original_values = {key: self._storage.get(key) for key in transaction.keys}

        try:
            # Execute operations
            for op in transaction.operations:
                if op['op'] == 'set':
                    self.set(op['key'], op['value'])
                elif op['op'] == 'delete':
                    self.delete(op['key'])
            TRANSACTIONS_COMMITTED.inc()
        except Exception as e:
            # Rollback changes
            for key, value in original_values.items():
                if value is None:
                    self._storage.delete(key)
                else:
                    self._storage.set(key, value)

            print(f"Transaction failed, rolling back: {e}")
            TRANSACTIONS_ROLLED_BACK.inc()
            del self._transactions[transaction_id]
            return False
        finally:
            # Release locks
            for lock in locks:
                lock.release()

        del self._transactions[transaction_id]

        return True

    def rollback(self, transaction_id):
        TRANSACTIONS_ROLLED_BACK.inc()
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

    def _compact(self):
        self._storage.close()
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
        REPLICATION_EVENTS.inc()
        for replica_url in self._replicas:
            try:
                payload = {'op': op, 'key': key}
                if value is not None:
                    payload['value'] = value

                requests.post(f"{replica_url}/replicate", json=payload, timeout=0.5)
            except requests.RequestException as e:
                print(f"Error replicating to {replica_url}: {e}")

    def atomic_update(self, key, update_function):
        lock = self._get_lock(key)
        with lock:
            current_value = self._storage.get(key)
            new_value = update_function(current_value)
            self.set(key, new_value)

    def get(self, key):
        GET_OPS.inc()
        return self._storage.get(key)

    def _update_indexes_for_set(self, key, value):
        for index_name, index_data in self._indexes.items():
            field = index_data['field']
            if isinstance(value, dict) and field in value:
                field_value = value[field]
                if field_value not in index_data['index']:
                    index_data['index'][field_value] = []
                if key not in index_data['index'][field_value]:
                    index_data['index'][field_value].append(key)

    def _update_indexes_for_delete(self, key, old_value):
        for index_name, index_data in self._indexes.items():
            field = index_data['field']
            if isinstance(old_value, dict) and field in old_value:
                field_value = old_value[field]
                if field_value in index_data['index'] and key in index_data['index'][field_value]:
                    index_data['index'][field_value].remove(key)
                    if not index_data['index'][field_value]:
                        del index_data['index'][field_value]

    def set(self, key, value, replicated=False):
        SET_OPS.inc()
        old_value = self._storage.get(key)
        self._storage.set(key, value)
        TOTAL_KEYS.set(len(list(self._storage.keys())))

        if old_value:
            self._update_indexes_for_delete(key, old_value)
        self._update_indexes_for_set(key, value)

        if self._role == 'primary':
            self._append_to_wal('set', key, value)
            self._replicate('set', key, value)
        elif replicated:
            self._append_to_wal('set', key, value)

    def query(self, index_name, value):
        QUERIES.inc()
        if index_name not in self._indexes:
            raise ValueError(f"Index '{index_name}' does not exist.")

        index = self._indexes[index_name]['index']
        if value in index:
            keys = index[value]
            return {key: self._storage.get(key) for key in keys}
        else:
            return {}

    def delete(self, key, replicated=False):
        old_value = self._storage.get(key)
        if old_value is not None:
            DELETE_OPS.inc()
            self._storage.delete(key)
            TOTAL_KEYS.set(len(list(self._storage.keys())))
            self._update_indexes_for_delete(key, old_value)

            if self._role == 'primary':
                self._append_to_wal('delete', key)
                self._replicate('delete', key)
            elif replicated:
                self._append_to_wal('delete', key)
            return True
        return False
