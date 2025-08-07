import os
import requests
from flask import Flask, request, jsonify
from uhashring import HashRing

app = Flask(__name__)

import argparse

# This will be initialized based on command-line arguments
ring = None

def get_shard(key):
    return ring.get_node(key)

@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    try:
        shard_url = get_shard(key)
        response = requests.get(f"{shard_url}/get/{key}")
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
        shard_url = get_shard(key)
        response = requests.post(f"{shard_url}/set", json=data)
        return response.content, response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/delete/<key>', methods=['DELETE'])
def delete_key(key):
    try:
        shard_url = get_shard(key)
        response = requests.delete(f"{shard_url}/delete/{key}")
        return response.content, response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--shards', type=str, required=True)
    parser.add_argument('--shard-weights', type=str)
    args = parser.parse_args()

    shards = args.shards.split(',')
    shard_weights = args.shard_weights.split(',') if args.shard_weights else []

    if shard_weights:
        nodes = {shards[i]: int(shard_weights[i]) for i in range(len(shards))}
    else:
        nodes = shards

    ring = HashRing(nodes=nodes)

    app.run(debug=True, port=args.port, use_reloader=False)
