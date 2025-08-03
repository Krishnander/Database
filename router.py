import os
import requests
from flask import Flask, request, jsonify
from uhashring import HashRing

app = Flask(__name__)

# The addresses of the primary nodes of each shard
SHARDS = os.environ.get('SHARDS', '').split(',') if os.environ.get('SHARDS') else []

ring = HashRing(nodes=SHARDS)

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
    app.run(debug=True, port=8000, use_reloader=False)
