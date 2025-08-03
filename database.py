import json
import os

class KeyValueStore:
    def __init__(self, db_file='database.json', wal_max_entries=100):
        self._db_file = db_file
        self._wal_file = db_file + '.wal'
        self._wal_max_entries = wal_max_entries
        self._wal_entries = 0
        self._data = self._load_from_disk()

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

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value
        self._append_to_wal('set', key, value)

    def delete(self, key):
        if key in self._data:
            del self._data[key]
            self._append_to_wal('delete', key)
            return True
        return False
