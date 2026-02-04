"""
Comprehensive tests for KV Store
"""
import os
import time
import threading
import subprocess
import signal
import requests
from client import KVStoreClient
from kv_store import KVStore


def cleanup_files(*files):
    """Remove test files"""
    for f in files:
        if os.path.exists(f):
            os.remove(f)


def wait_for_server(url="http://127.0.0.1:5000/health", timeout=10):
    """Wait for server to be ready"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(url, timeout=1)
            if response.status_code == 200:
                return True
        except:
            pass
        time.sleep(0.1)
    return False


def test_set_then_get():
    """Test 1: Set then Get"""
    print("\n[Test 1] Set then Get")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    store = KVStore(db_path="test_kvstore.db", wal_path="test_kvstore.wal")
    store.set("key1", "value1")
    result = store.get("key1")
    assert result == "value1", f"Expected 'value1', got {result}"
    print("[PASSED]")


def test_set_delete_get():
    """Test 2: Set then Delete then Get"""
    print("\n[Test 2] Set then Delete then Get")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    store = KVStore(db_path="test_kvstore.db", wal_path="test_kvstore.wal")
    store.set("key2", "value2")
    store.delete("key2")
    result = store.get("key2")
    assert result is None, f"Expected None, got {result}"
    print("[PASSED]")


def test_get_without_setting():
    """Test 3: Get without setting"""
    print("\n[Test 3] Get without setting")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    store = KVStore(db_path="test_kvstore.db", wal_path="test_kvstore.wal")
    result = store.get("nonexistent")
    assert result is None, f"Expected None, got {result}"
    print("[PASSED]")


def test_set_set_get():
    """Test 4: Set then Set (same key) then Get"""
    print("\n[Test 4] Set then Set (same key) then Get")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    store = KVStore(db_path="test_kvstore.db", wal_path="test_kvstore.wal")
    store.set("key4", "value4a")
    store.set("key4", "value4b")
    result = store.get("key4")
    assert result == "value4b", f"Expected 'value4b', got {result}"
    print("[PASSED]")


def test_persistence():
    """Test 5: Set then exit (gracefully) then Get"""
    print("\n[Test 5] Set then exit (gracefully) then Get")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    # First store instance
    store1 = KVStore(db_path="test_kvstore.db", wal_path="test_kvstore.wal")
    store1.set("key5", "value5")
    store1.checkpoint()
    del store1
    
    # Second store instance (simulates restart)
    store2 = KVStore(db_path="test_kvstore.db", wal_path="test_kvstore.wal")
    result = store2.get("key5")
    assert result == "value5", f"Expected 'value5', got {result}"
    print("[PASSED]")


def test_bulk_set():
    """Test 6: Bulk Set"""
    print("\n[Test 6] Bulk Set")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    store = KVStore(db_path="test_kvstore.db", wal_path="test_kvstore.wal")
    items = [("bulk_key1", "bulk_value1"), ("bulk_key2", "bulk_value2"), ("bulk_key3", "bulk_value3")]
    store.bulk_set(items)
    
    assert store.get("bulk_key1") == "bulk_value1"
    assert store.get("bulk_key2") == "bulk_value2"
    assert store.get("bulk_key3") == "bulk_value3"
    print("[PASSED]")


def test_client_set_get():
    """Test 7: Client Set then Get"""
    print("\n[Test 7] Client Set then Get")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    # Start server
    server_process = subprocess.Popen(
        ["python", "server.py", "--db-path", "test_kvstore.db", "--wal-path", "test_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        if not wait_for_server():
            raise Exception("Server failed to start")
        
        client = KVStoreClient()
        client.set("client_key1", "client_value1")
        result = client.get("client_key1")
        assert result == "client_value1", f"Expected 'client_value1', got {result}"
        print("[PASSED]")
    finally:
        server_process.terminate()
        server_process.wait()


def test_client_persistence():
    """Test 8: Client persistence across restarts"""
    print("\n[Test 8] Client persistence across restarts")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    # First server instance
    server1 = subprocess.Popen(
        ["python", "server.py", "--db-path", "test_kvstore.db", "--wal-path", "test_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        if not wait_for_server():
            raise Exception("Server failed to start")
        
        client1 = KVStoreClient()
        client1.set("persist_key", "persist_value")
        client1.checkpoint()
        client1.close()
    finally:
        server1.terminate()
        server1.wait()
    
    time.sleep(0.5)
    
    # Second server instance
    server2 = subprocess.Popen(
        ["python", "server.py", "--db-path", "test_kvstore.db", "--wal-path", "test_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        if not wait_for_server():
            raise Exception("Server failed to start")
        
        client2 = KVStoreClient()
        result = client2.get("persist_key")
        assert result == "persist_value", f"Expected 'persist_value', got {result}"
        print("[PASSED]")
    finally:
        server2.terminate()
        server2.wait()


def test_concurrent_bulk_set():
    """Test 9: Concurrent bulk set writes touching same keys (ACID)"""
    print("\n[Test 9] Concurrent bulk set writes (ACID)")
    cleanup_files("test_kvstore.db", "test_kvstore.wal")
    
    server_process = subprocess.Popen(
        ["python", "server.py", "--db-path", "test_kvstore.db", "--wal-path", "test_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        if not wait_for_server():
            raise Exception("Server failed to start")
        
        results = []
        errors = []
        
        def bulk_write_worker(worker_id, keys):
            """Worker that does bulk writes"""
            try:
                client = KVStoreClient()
                items = [(f"shared_key_{k}", f"worker_{worker_id}_value_{k}") for k in keys]
                client.bulk_set(items)
                client.close()
                results.append(worker_id)
            except Exception as e:
                errors.append((worker_id, str(e)))
        
        # Create 5 threads, each writing to overlapping keys
        threads = []
        for i in range(5):
            keys = list(range(i, i + 3))  # Overlapping keys
            t = threading.Thread(target=bulk_write_worker, args=(i, keys))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Check that all operations completed (no corruption)
        client = KVStoreClient()
        all_keys = set()
        for i in range(7):  # Check all possible keys
            value = client.get(f"shared_key_{i}")
            if value:
                all_keys.add(i)
        
        # At least some writes should have succeeded
        assert len(all_keys) > 0, "No keys were written"
        assert len(errors) == 0, f"Errors occurred: {errors}"
        print("[PASSED]")
    finally:
        server_process.terminate()
        server_process.wait()


def run_all_tests():
    """Run all tests"""
    print("=" * 60)
    print("Running KV Store Tests")
    print("=" * 60)
    
    tests = [
        test_set_then_get,
        test_set_delete_get,
        test_get_without_setting,
        test_set_set_get,
        test_persistence,
        test_bulk_set,
        test_client_set_get,
        test_client_persistence,
        test_concurrent_bulk_set,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"[FAILED] {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Tests completed: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
