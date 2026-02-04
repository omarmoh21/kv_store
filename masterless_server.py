"""
Master-less HTTP Server for KV Store
"""
from flask import Flask, request, jsonify
from masterless import MasterlessKVStore, MasterlessNode
import threading
import signal
import sys
import json

app = Flask(__name__)
masterless_store = None


def create_masterless_store(node_id: str, nodes_config: list, db_path: str, wal_path: str):
    """Create master-less KV store"""
    global masterless_store
    
    # Parse nodes configuration
    nodes = []
    for node_config in nodes_config:
        node = MasterlessNode(
            node_id=node_config['id'],
            host=node_config['host'],
            port=node_config['port']
        )
        nodes.append(node)
    
    # Create master-less store
    masterless_store = MasterlessKVStore(node_id, nodes, db_path, wal_path)
    
    return masterless_store


@app.route('/set', methods=['POST'])
def set_key():
    """Set a key-value pair"""
    try:
        data = request.json
        key = data.get('key')
        value = data.get('value')
        debug = data.get('debug', None)
        
        if key is None:
            return jsonify({'status': 'error', 'error': 'key is required'}), 400
        
        masterless_store.set(key, value, debug=debug)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    """Get value by key"""
    try:
        consistency = request.args.get('consistency', 'eventual')
        value = masterless_store.get(key, read_consistency=consistency)
        return jsonify({'status': 'ok', 'value': value})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/delete/<key>', methods=['DELETE'])
def delete_key(key):
    """Delete a key"""
    try:
        masterless_store.delete(key)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/bulk_set', methods=['POST'])
def bulk_set_keys():
    """Bulk set multiple key-value pairs"""
    try:
        data = request.json
        items = data.get('items', [])
        debug = data.get('debug', None)
        
        if not isinstance(items, list):
            return jsonify({'status': 'error', 'error': 'items must be a list'}), 400
        
        masterless_store.bulk_set(items, debug=debug)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/masterless/replicate', methods=['POST'])
def receive_replication():
    """Receive replicated data from another node"""
    try:
        data = request.json
        source_node_id = data.get('node_id')
        replicated_data = data.get('data', {})
        
        masterless_store.receive_replication(source_node_id, replicated_data)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/masterless/get/<key>', methods=['GET'])
def masterless_get(key):
    """Get value with version info (for replication)"""
    try:
        value = masterless_store.store.get(key)
        clock = masterless_store.versioned_data.get(key, (None, {}))[1]
        return jsonify({'status': 'ok', 'value': value, 'clock': clock})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/checkpoint', methods=['POST'])
def checkpoint():
    """Create a checkpoint"""
    try:
        masterless_store.checkpoint()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'node_id': masterless_store.node_id,
        'mode': 'masterless'
    })


def signal_handler(sig, frame):
    """Handle shutdown gracefully"""
    print("\nShutting down master-less server...")
    if masterless_store:
        masterless_store.stop()
        masterless_store.checkpoint()
    sys.exit(0)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='KV Store Master-less Server')
    parser.add_argument('--node-id', required=True, help='Node ID')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, required=True, help='Port to bind to')
    parser.add_argument('--nodes', required=True, help='JSON file with nodes configuration')
    parser.add_argument('--db-path', default='kvstore.db', help='Database file path')
    parser.add_argument('--wal-path', default='kvstore.wal', help='WAL file path')
    
    args = parser.parse_args()
    
    # Load nodes configuration
    with open(args.nodes, 'r') as f:
        nodes_config = json.load(f)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create master-less store
    db_path = args.db_path.replace('.db', f'_{args.node_id}.db')
    wal_path = args.wal_path.replace('.wal', f'_{args.node_id}.wal')
    create_masterless_store(args.node_id, nodes_config, db_path, wal_path)
    
    print(f"Master-less KV Store server starting on http://{args.host}:{args.port}")
    print(f"Node ID: {args.node_id}")
    print(f"Database: {db_path}, WAL: {wal_path}")
    
    app.run(host=args.host, port=args.port, threaded=True)
