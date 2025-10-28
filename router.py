import os
import requests
from flask import Flask, request, jsonify
from uhashring import HashRing

app = Flask(__name__)

import argparse

# This will be initialized based on command-line arguments
ring = None
shards = {}
leader_cache = {}

def get_leader(shard_name):
    # NOTE: This is a simplified leader discovery mechanism.
    # In a production system, this cache would need to be more robust.
    # For example, it should have a Time-To-Live (TTL) to periodically
    # re-validate the leader, and a mechanism to invalidate the cache
    # immediately if a request to the cached leader fails.
    if shard_name in leader_cache:
        # Quick check to see if the cached leader is still alive.
        # This is a simple, but not foolproof, way to handle failover.
        try:
            response = requests.get(f"{leader_cache[shard_name]}/status", timeout=0.2)
            if response.status_code == 200 and response.json().get('leader'):
                return leader_cache[shard_name]
            else:
                # The cached leader is no longer the leader, remove from cache.
                del leader_cache[shard_name]
        except requests.RequestException:
             # The cached leader is unreachable, remove from cache.
            del leader_cache[shard_name]

    nodes = shards[shard_name]
    for node_url in nodes:
        try:
            response = requests.get(f"{node_url}/status", timeout=0.5)
            if response.status_code == 200 and response.json().get('leader'):
                leader_cache[shard_name] = node_url
                return node_url
        except requests.RequestException:
            continue
    return None # No leader found

def get_any_node(shard_name):
    import random
    return random.choice(shards[shard_name])

@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    try:
        shard_name = ring.get_node(key)
        node_url = get_any_node(shard_name)
        headers = {'Authorization': request.headers.get('Authorization')}
        response = requests.get(f"{node_url}/get/{key}", headers=headers)
        return response.content, response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/set', methods=['POST'])
def set_key():
    data = request.get_json()
    key = data.get('key')
    if not key:
        return jsonify({'error': 'Key is required.'}), 400

    try:
        shard_name = ring.get_node(key)
        leader_url = get_leader(shard_name)
        if not leader_url:
            return jsonify({'error': f'No leader found for shard {shard_name}.'}), 503
        headers = {'Authorization': request.headers.get('Authorization'), 'Content-Type': 'application/json'}
        response = requests.post(f"{leader_url}/set", json=data, headers=headers)
        return response.content, response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/delete/<key>', methods=['DELETE'])
def delete_key(key):
    try:
        shard_name = ring.get_node(key)
        leader_url = get_leader(shard_name)
        if not leader_url:
            return jsonify({'error': f'No leader found for shard {shard_name}.'}), 503
        headers = {'Authorization': request.headers.get('Authorization')}
        response = requests.delete(f"{leader_url}/delete/{key}", headers=headers)
        return response.content, response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--shards', type=str, required=True, help='A comma-separated list of shards, where each shard is a semicolon-separated list of nodes.')
    args = parser.parse_args()

    shards_config = args.shards.split(',')
    shards = {}
    for i, shard_nodes in enumerate(shards_config):
        shard_name = f'shard{i}'
        shards[shard_name] = shard_nodes.split(';')

    ring = HashRing(nodes=list(shards.keys()))

    app.run(debug=True, port=args.port, use_reloader=False)
