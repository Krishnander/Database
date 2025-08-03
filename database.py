import json
import os

class KeyValueStore:
    def __init__(self, db_file='database.json'):
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
        self._save_to_disk()

    def delete(self, key):
        if key in self._data:
            del self._data[key]
            self._save_to_disk()
            return True
        return False
