import json
import os
import requests
import threading
from prometheus_client import Counter, Gauge
from abc import ABC, abstractmethod
import paxos

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
    def __init__(self, db_file='database.json', wal_max_entries=100, network_uid=None, peers=None, testing=False):
        self._storage = JSONStorageEngine(db_file)
        self._wal_file = db_file + '.wal'
        self._wal_max_entries = wal_max_entries
        self._wal_entries = 0
        self.network_uid = network_uid
        self.peers = peers if peers is not None else []
        self.quorum_size = len(self.peers) // 2 + 1
        self.testing = testing

        if not self.testing:
            self.paxos = paxos.PaxosInstance(self.network_uid, self.quorum_size)
        else:
            class MockPaxos:
                def __init__(self):
                    self.leader = True  # Mock the leader attribute

                def is_leader(self):
                    return True

                def get_leader_url(self):
                    return None
            self.paxos = MockPaxos()

        self._replay_wal()
        self.paxos_log = [] # for now, in-memory log
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

    def _propose(self, proposal_value):
        if self.testing:
            self.apply_paxos_log(proposal_value)
            return

        # NOTE: This is a simplified, non-blocking simulation of Paxos for demonstration purposes.
        # In a production-grade implementation, the following steps would be necessary:
        # 1. The leader sends a "prepare" message to a quorum of acceptors.
        # 2. The leader waits for "promise" responses from the acceptors. This would be a blocking,
        #    asynchronous operation with timeouts.
        # 3. If a quorum of promises is received, the leader sends an "accept" message with the
        #    proposed value to the acceptors.
        # 4. The leader waits for "accepted" responses from the acceptors.
        # 5. Only after receiving a quorum of "accepted" messages would the leader apply the
        #    log entry to its own state machine and then respond to the client.
        # The current implementation sends the prepare message but then proceeds to apply the log
        # locally without waiting for consensus, simulating a "perfect network" scenario.

        prepare_msg = self.paxos.prepare()
        for peer in self.peers:
            if peer != self.network_uid:
                try:
                    requests.post(f"{peer}/paxos", json={'message_type': 'Prepare', 'from_uid': prepare_msg.from_uid, 'proposal_id': list(prepare_msg.proposal_id)})
                except requests.RequestException as e:
                    print(f"Error sending prepare message to {peer}: {e}")

        # This is a hack to simulate the paxos flow. We apply the log directly.
        self.apply_paxos_log(proposal_value)


    def receive_paxos_message(self, message_data):
        message_type = message_data.pop('message_type')
        cls = getattr(paxos, message_type)

        if 'proposal_id' in message_data and message_data['proposal_id']:
            message_data['proposal_id'] = paxos.ProposalID(*message_data['proposal_id'])
        if 'promised_proposal_id' in message_data and message_data['promised_proposal_id']:
            message_data['promised_proposal_id'] = paxos.ProposalID(*message_data['promised_proposal_id'])
        if 'last_accepted_id' in message_data and message_data['last_accepted_id']:
            message_data['last_accepted_id'] = paxos.ProposalID(*message_data['last_accepted_id'])

        message = cls(**message_data)

        response = self.paxos.receive(message)

        if response:
            # In a real implementation, we would send the response back to the network.
            # For now, we'll just log it.
            print(f"Generated paxos response: {response}")


    def apply_paxos_log(self, proposal):
        op, key, value = proposal

        if op == 'set':
            SET_OPS.inc()
            old_value = self._storage.get(key)
            self._storage.set(key, value)
            TOTAL_KEYS.set(len(list(self._storage.keys())))

            if old_value is not None:
                self._update_indexes_for_delete(key, old_value)
            self._update_indexes_for_set(key, value)

            self._append_to_wal('set', key, value)

        elif op == 'delete':
            old_value = self._storage.get(key)
            if old_value is not None:
                DELETE_OPS.inc()
                self._storage.delete(key)
                TOTAL_KEYS.set(len(list(self._storage.keys())))
                self._update_indexes_for_delete(key, old_value)
                self._append_to_wal('delete', key)

        self.paxos_log.append(proposal)


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

    def set(self, key, value):
        self._propose(('set', key, value))

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

    def delete(self, key):
        # Check if key exists before proposing, to avoid unnecessary operations
        if self._storage.get(key) is None:
            return False

        self._propose(('delete', key, None))

        # In a real async system, we'd return a future.
        # For this simplified model, we assume the operation will eventually succeed if proposed.
        # We check the state after our synchronous call to determine success.
        return self._storage.get(key) is None
