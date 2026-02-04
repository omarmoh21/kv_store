"""
Example usage of KV Store with all features
"""
from kv_store import KVStore
from client import KVStoreClient
from indexes import IndexedKVStore
import subprocess
import time
import requests


def example_basic_usage():
    """Example: Basic KV Store usage"""
    print("\n" + "=" * 60)
    print("Example 1: Basic KV Store Usage")
    print("=" * 60)
    
    # Create store
    store = KVStore(db_path="example.db", wal_path="example.wal")
    
    # Set values
    store.set("name", "Alice")
    store.set("age", 30)
    store.set("city", "New York")
    
    # Get values
    print(f"Name: {store.get('name')}")
    print(f"Age: {store.get('age')}")
    print(f"City: {store.get('city')}")
    
    # Delete
    store.delete("age")
    print(f"Age after delete: {store.get('age')}")
    
    # Bulk set
    store.bulk_set([("key1", "value1"), ("key2", "value2"), ("key3", "value3")])
    print(f"Key1: {store.get('key1')}")
    print(f"Key2: {store.get('key2')}")
    
    # Checkpoint
    store.checkpoint()
    print("\n[SUCCESS] Basic operations completed")


def example_client_server():
    """Example: Client-Server usage"""
    print("\n" + "=" * 60)
    print("Example 2: Client-Server Usage")
    print("=" * 60)
    
    # Start server (in background)
    server = subprocess.Popen(
        ["python", "server.py", "--port", "5000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        # Wait for server to start
        for i in range(10):
            try:
                response = requests.get("http://127.0.0.1:5000/health", timeout=1)
                if response.status_code == 200:
                    break
            except:
                pass
            time.sleep(0.5)
        
        # Use client
        client = KVStoreClient(host="127.0.0.1", port=5000)
        
        # Set values
        client.set("server_key1", "server_value1")
        client.set("server_key2", "server_value2")
        
        # Get values
        print(f"Server Key1: {client.get('server_key1')}")
        print(f"Server Key2: {client.get('server_key2')}")
        
        # Bulk set
        client.bulk_set([("bulk1", "val1"), ("bulk2", "val2")])
        print(f"Bulk1: {client.get('bulk1')}")
        
        client.close()
        print("\n[SUCCESS] Client-Server operations completed")
    
    finally:
        server.terminate()
        server.wait()


def example_indexed_store():
    """Example: Indexed KV Store with full-text search"""
    print("\n" + "=" * 60)
    print("Example 3: Indexed KV Store")
    print("=" * 60)
    
    # Create base store
    base_store = KVStore(db_path="indexed.db", wal_path="indexed.wal")
    
    # Create indexed store
    indexed_store = IndexedKVStore(base_store, enable_fulltext=True, enable_embeddings=True)
    
    # Add documents
    indexed_store.set("doc1", "Python programming language tutorial")
    indexed_store.set("doc2", "Java development best practices")
    indexed_store.set("doc3", "Python machine learning algorithms")
    indexed_store.set("doc4", "JavaScript web development guide")
    
    # Full-text search
    print("\nFull-text search for 'Python':")
    results = indexed_store.search_fulltext("Python", limit=5)
    for key, score in results:
        print(f"  {key}: {score:.4f}")
    
    # Similarity search
    print("\nSimilarity search for 'programming':")
    results = indexed_store.search_similar("programming", limit=5)
    for key, score in results:
        print(f"  {key}: {score:.4f}")
    
    print("\n[SUCCESS] Indexed store operations completed")


def example_debug_mode():
    """Example: Debug mode for simulating sync issues"""
    print("\n" + "=" * 60)
    print("Example 4: Debug Mode (Simulating Sync Issues)")
    print("=" * 60)
    
    # Create store with debug mode
    store = KVStore(db_path="debug.db", wal_path="debug.wal", debug=True)
    
    # Set values (some snapshots might fail due to debug mode)
    for i in range(10):
        store.set(f"debug_key_{i}", f"debug_value_{i}")
    
    print("Set 10 keys with debug mode enabled")
    print("(Some snapshots may fail with 1% probability)")
    
    # Checkpoint (might fail)
    store.checkpoint()
    print("\n[SUCCESS] Debug mode demonstration completed")


if __name__ == "__main__":
    print("=" * 60)
    print("KV Store Examples")
    print("=" * 60)
    
    try:
        example_basic_usage()
        example_client_server()
        example_indexed_store()
        example_debug_mode()
        
        print("\n" + "=" * 60)
        print("[SUCCESS] All examples completed!")
        print("=" * 60)
    
    except Exception as e:
        print(f"\n[ERROR] Example failed: {e}")
        import traceback
        traceback.print_exc()
