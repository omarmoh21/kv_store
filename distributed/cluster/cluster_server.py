"""
Cluster-aware HTTP Server for KV Store.
"""
from flask import Flask, request, jsonify
from .cluster import ClusterKVStore, ClusterManager, ClusterNode
import threading
import signal
import sys
import os

app = Flask(__name__)
cluster_store = None


def create_cluster_store(node_id: str, nodes_config: list, db_path: str, wal_path: str):
    """Create cluster-aware KV store"""
    global cluster_store
    
    # Parse nodes configuration
    nodes = []
    for node_config in nodes_config:
        node = ClusterNode(
            node_id=node_config['id'],
            host=node_config['host'],
            port=node_config['port'],
            is_primary=node_config.get('is_primary', False)
        )
        nodes.append(node)
    
    # Create cluster manager
    cluster_manager = ClusterManager(nodes, node_id)
    
    # Create cluster store
    cluster_store = ClusterKVStore(cluster_manager, db_path, wal_path)
    
    return cluster_store


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
        
        cluster_store.set(key, value, debug=debug)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    """Get value by key"""
    try:
        value = cluster_store.get(key)
        return jsonify({'status': 'ok', 'value': value})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/delete/<key>', methods=['DELETE'])
def delete_key(key):
    """Delete a key"""
    try:
        cluster_store.delete(key)
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
        
        cluster_store.bulk_set(items, debug=debug)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/checkpoint', methods=['POST'])
def checkpoint():
    """Create a checkpoint"""
    try:
        cluster_store.checkpoint()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'node_id': cluster_store.cluster_manager.current_node_id,
        'is_primary': cluster_store.cluster_manager.primary_node_id == cluster_store.cluster_manager.current_node_id
    })


@app.route('/cluster/status', methods=['GET'])
def cluster_status():
    """Get cluster status"""
    try:
        status = {
            'current_node': cluster_store.cluster_manager.current_node_id,
            'primary_node': cluster_store.cluster_manager.primary_node_id,
            'nodes': {}
        }
        
        for node_id, node in cluster_store.cluster_manager.nodes.items():
            status['nodes'][node_id] = {
                'host': node.host,
                'port': node.port,
                'is_primary': node.is_primary,
                'is_alive': node.is_alive
            }
        
        return jsonify(status)
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


def signal_handler(sig, frame):
    """Handle shutdown gracefully"""
    print("\nShutting down cluster server...")
    if cluster_store:
        cluster_store.checkpoint()
    sys.exit(0)


if __name__ == '__main__':
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='KV Store Cluster Server')
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
    
    # Create cluster store
    db_path = args.db_path.replace('.db', f'_{args.node_id}.db')
    wal_path = args.wal_path.replace('.wal', f'_{args.node_id}.wal')
    create_cluster_store(args.node_id, nodes_config, db_path, wal_path)
    
    print(f"Cluster KV Store server starting on http://{args.host}:{args.port}")
    print(f"Node ID: {args.node_id}")
    print(f"Database: {db_path}, WAL: {wal_path}")
    
    app.run(host=args.host, port=args.port, threaded=True)
