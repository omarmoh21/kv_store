"""
HTTP Server for KV Store
Built on top of TCP using Flask for simplicity.
"""
from flask import Flask, request, jsonify
from .kv_store import KVStore
import threading
import signal
import sys

app = Flask(__name__)
store = None


def create_store(db_path="kvstore.db", wal_path="kvstore.wal", debug=False):
    """Create and initialize the KV store"""
    global store
    store = KVStore(db_path=db_path, wal_path=wal_path, debug=debug)
    return store


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
        
        store.set(key, value, debug=debug)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    """Get value by key"""
    try:
        value = store.get(key)
        return jsonify({'status': 'ok', 'value': value})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/delete/<key>', methods=['DELETE'])
def delete_key(key):
    """Delete a key"""
    try:
        store.delete(key)
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
            return jsonify({'status': 'error', 'error': 'items must be a list of [key, value] pairs'}), 400
        
        store.bulk_set(items, debug=debug)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/checkpoint', methods=['POST'])
def checkpoint():
    """Create a checkpoint"""
    try:
        store.checkpoint()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'message': 'KV Store is running'})


def signal_handler(sig, frame):
    """Handle shutdown gracefully"""
    print("\nShutting down server...")
    if store:
        store.checkpoint()
    sys.exit(0)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='KV Store HTTP Server')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--db-path', default='kvstore.db', help='Database file path')
    parser.add_argument('--wal-path', default='kvstore.wal', help='WAL file path')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode (simulate sync issues)')
    
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create store
    create_store(db_path=args.db_path, wal_path=args.wal_path, debug=args.debug)
    
    print(f"KV Store server starting on http://{args.host}:{args.port}")
    print(f"Database: {args.db_path}, WAL: {args.wal_path}")
    if args.debug:
        print("Debug mode enabled (1% chance of sync failures)")
    
    app.run(host=args.host, port=args.port, threaded=True)
