from flask import Flask, request, jsonify
from database import KeyValueStore

app = Flask(__name__)
db = KeyValueStore('database.json')

@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    value = db.get(key)
    if value is not None:
        return jsonify({key: value})
    else:
        return jsonify({'error': 'Key not found'}), 404

@app.route('/set', methods=['POST'])
def set_key():
    data = request.get_json()
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({'error': 'Invalid request. "key" and "value" are required.'}), 400
    key = data['key']
    value = data['value']
    db.set(key, value)
    return jsonify({'message': f'Key "{key}" set successfully.'})

@app.route('/delete/<key>', methods=['DELETE'])
def delete_key(key):
    if db.delete(key):
        return jsonify({'message': f'Key "{key}" deleted successfully.'})
    else:
        return jsonify({'error': 'Key not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)
