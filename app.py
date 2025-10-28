from flask import Flask, request, jsonify
from database import KeyValueStore
import os
import argparse
import requests
import sys

app = Flask(__name__)

from functools import wraps

# This will be initialized based on command-line arguments
db = None
VALID_API_KEYS = os.environ.get('API_KEYS', '').split(',') if os.environ.get('API_KEYS') else []

def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'Authorization' not in request.headers:
            return jsonify({'error': 'Authorization header is missing.'}), 401

        api_key = request.headers['Authorization']
        if api_key not in VALID_API_KEYS:
            return jsonify({'error': 'Invalid API key.'}), 401

        return f(*args, **kwargs)
    return decorated_function

@app.route('/get/<key>', methods=['GET'])
@require_api_key
def get_key(key):
    value = db.get(key)
    if value is not None:
        return jsonify(value=value)
    else:
        return jsonify({'error': 'Key not found'}), 404

@app.route('/set', methods=['POST'])
@require_api_key
def set_key():
    if not db.paxos.leader:
        return jsonify({'error': 'Not the leader. Please send write requests to the leader.'}), 403
    data = request.get_json()
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({'error': 'Invalid request. "key" and "value" are required.'}), 400
    key = data['key']
    value = data['value']
    db.set(key, value)
    return jsonify({'message': f'Key "{key}" set successfully.'})

@app.route('/delete/<key>', methods=['DELETE'])
@require_api_key
def delete_key(key):
    if not db.paxos.leader:
        return jsonify({'error': 'Not the leader. Please send write requests to the leader.'}), 403
    if db.delete(key):
        return jsonify({'message': f'Key "{key}" deleted successfully.'})
    else:
        return jsonify({'error': 'Key not found'}), 404

@app.route('/paxos', methods=['POST'])
def paxos():
    message_data = request.get_json()
    # In a real implementation, we would handle the response and send it back to the network
    db.receive_paxos_message(message_data)
    return jsonify({'message': 'ok'})


@app.route('/transaction/begin', methods=['POST'])
@require_api_key
def begin_transaction():
    if not db.paxos.leader:
        return jsonify({'error': 'Not the leader. Please send write requests to the leader.'}), 403
    transaction_id = db.begin()
    return jsonify({'transaction_id': transaction_id})

@app.route('/transaction/add_op', methods=['POST'])
@require_api_key
def add_transaction_op():
    if not db.paxos.leader:
        return jsonify({'error': 'Not the leader. Please send write requests to the leader.'}), 403
    data = request.get_json()
    transaction_id = data.get('transaction_id')
    op = data.get('op')
    key = data.get('key')
    value = data.get('value')

    try:
        db.add_op(transaction_id, op, key, value)
        return jsonify({'message': 'Operation added to transaction.'})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

@app.route('/transaction/commit', methods=['POST'])
@require_api_key
def commit_transaction():
    if not db.paxos.leader:
        return jsonify({'error': 'Not the leader. Please send write requests to the leader.'}), 403
    data = request.get_json()
    transaction_id = data.get('transaction_id')

    try:
        if db.commit(transaction_id):
            return jsonify({'message': 'Transaction committed successfully.'})
        else:
            return jsonify({'error': 'Transaction failed and was rolled back.'}), 500
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

@app.route('/transaction/rollback', methods=['POST'])
@require_api_key
def rollback_transaction():
    if not db.paxos.leader:
        return jsonify({'error': 'Not the leader. Please send write requests to the leader.'}), 403
    data = request.get_json()
    transaction_id = data.get('transaction_id')

    try:
        db.rollback(transaction_id)
        return jsonify({'message': 'Transaction rolled back successfully.'})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400


@app.route('/index/create', methods=['POST'])
@require_api_key
def create_index():
    data = request.get_json()
    index_name = data.get('index_name')
    field = data.get('field')

    try:
        db.create_index(index_name, field)
        return jsonify({'message': f"Index '{index_name}' created successfully."})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        'leader': db.paxos.leader,
        'network_uid': db.network_uid
    })

@app.route('/force_leader', methods=['POST'])
def force_leader():
    db.paxos.leader = True
    return jsonify({'message': 'OK'})

@app.route('/query', methods=['GET'])
@require_api_key
def query():
    index_name = request.args.get('index_name')
    value = request.args.get('value')

    # Since query params are strings, we need to try to convert them to numbers
    try:
        value = int(value)
    except (ValueError, TypeError):
        try:
            value = float(value)
        except (ValueError, TypeError):
            pass

    try:
        results = db.query(index_name, value)
        return jsonify(results)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400


from prometheus_client import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

# Add prometheus wsgi middleware to route /metrics requests
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/metrics': make_wsgi_app()
})

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--peers', type=str, required=True)
    args = parser.parse_args()

    my_address = f"http://localhost:{args.port}"
    peers = args.peers.split(',')

    # Use a unique database file for each instance
    db_file = f'database_{args.port}.json'

    db = KeyValueStore(db_file=db_file, network_uid=my_address, peers=peers)

    # Manually set one node as the leader for now for testing purposes
    if args.port == 5000:
        db.paxos.leader = True

    app.run(debug=True, port=args.port, use_reloader=False)
