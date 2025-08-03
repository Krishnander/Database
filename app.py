from flask import Flask, request, jsonify
from database import KeyValueStore
import os
import argparse
import requests

app = Flask(__name__)

# This will be initialized based on command-line arguments
db = None

@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    value = db.get(key)
    if value is not None:
        return jsonify({key: value})
    else:
        return jsonify({'error': 'Key not found'}), 404

@app.route('/set', methods=['POST'])
def set_key():
    if db._role == 'replica':
        return jsonify({'error': 'Cannot set key on a replica.'}), 403
    data = request.get_json()
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({'error': 'Invalid request. "key" and "value" are required.'}), 400
    key = data['key']
    value = data['value']
    db.set(key, value)
    return jsonify({'message': f'Key "{key}" set successfully.'})

@app.route('/delete/<key>', methods=['DELETE'])
def delete_key(key):
    if db._role == 'replica':
        return jsonify({'error': 'Cannot delete key on a replica.'}), 403
    if db.delete(key):
        return jsonify({'message': f'Key "{key}" deleted successfully.'})
    else:
        return jsonify({'error': 'Key not found'}), 404

@app.route('/replicate', methods=['POST'])
def replicate():
    data = request.get_json()
    op = data.get('op')
    key = data.get('key')
    value = data.get('value')

    if op == 'set':
        db.set(key, value, replicated=True)
    elif op == 'delete':
        db.delete(key, replicated=True)
    else:
        return jsonify({'error': 'Invalid replication operation.'}), 400

    return jsonify({'message': 'Replication successful.'})

@app.route('/register_replica', methods=['POST'])
def register_replica():
    if db._role != 'primary':
        return jsonify({'error': 'Only the primary can register replicas.'}), 403

    data = request.get_json()
    replica_url = data.get('replica_url')
    if not replica_url:
        return jsonify({'error': 'replica_url is required.'}), 400

    db.add_replica(replica_url)
    return jsonify({'message': f'Replica {replica_url} registered successfully.'})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--role', type=str, default='primary', choices=['primary', 'replica'])
    parser.add_argument('--primary-address', type=str)
    parser.add_argument('--replicas', type=str)
    args = parser.parse_args()

    replicas = args.replicas.split(',') if args.replicas else []

    # Use a unique database file for each instance
    db_file = f'database_{args.port}.json'

    db = KeyValueStore(db_file=db_file, role=args.role, replicas=replicas)

    if args.role == 'replica':
        if not args.primary_address:
            raise ValueError('A primary address must be specified for a replica.')

        # Register with the primary
        try:
            my_address = f"http://localhost:{args.port}"
            requests.post(f"{args.primary_address}/register_replica", json={'replica_url': my_address})
        except requests.RequestException as e:
            print(f"Could not register with primary: {e}")


    app.run(debug=True, port=args.port, use_reloader=False)
