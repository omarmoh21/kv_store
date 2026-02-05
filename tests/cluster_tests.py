"""
Tests for Cluster Replication and Leader Election.
"""
import os
import time
import threading
import subprocess
import signal
import requests
import json

from core.client import KVStoreClient


def cleanup_files(*files):
    """Remove test files"""
    for f in files:
        if os.path.exists(f):
            os.remove(f)


def wait_for_server(url, timeout=10):
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


def create_nodes_config():
    """Create nodes configuration file"""
    config = [
        {'id': 'node1', 'host': '127.0.0.1', 'port': 5001, 'is_primary': True},
        {'id': 'node2', 'host': '127.0.0.1', 'port': 5002, 'is_primary': False},
        {'id': 'node3', 'host': '127.0.0.1', 'port': 5003, 'is_primary': False}
    ]
    
    with open('cluster_nodes.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    return config


def test_cluster_replication():
    """Test: Write to primary, read from secondary"""
    print("\n[Cluster Test 1] Replication: Write to primary, verify on secondaries")
    
    cleanup_files('cluster_nodes.json', 
                   'kvstore_node1.db', 'kvstore_node1.wal',
                   'kvstore_node2.db', 'kvstore_node2.wal',
                   'kvstore_node3.db', 'kvstore_node3.wal')
    
    # Create nodes config
    nodes_config = create_nodes_config()
    
    # Start all nodes
    processes = []
    for node in nodes_config:
        proc = subprocess.Popen(
            ['python', '-m', 'distributed.cluster.cluster_server',
             '--node-id', node['id'],
             '--host', node['host'],
             '--port', str(node['port']),
             '--nodes', 'cluster_nodes.json'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append((node['id'], proc))
    
    try:
        # Wait for all servers to start
        for node in nodes_config:
            url = f"http://{node['host']}:{node['port']}/health"
            if not wait_for_server(url, timeout=15):
                raise Exception(f"Server {node['id']} failed to start")
        
        time.sleep(2)  # Let cluster stabilize
        
        # Write to primary
        primary_client = KVStoreClient(host='127.0.0.1', port=5001)
        primary_client.set('replicated_key', 'replicated_value')
        
        time.sleep(1)  # Allow replication
        
        # Verify on secondaries (they should forward to primary)
        secondary1_client = KVStoreClient(host='127.0.0.1', port=5002)
        secondary2_client = KVStoreClient(host='127.0.0.1', port=5003)
        
        value1 = secondary1_client.get('replicated_key')
        value2 = secondary2_client.get('replicated_key')
        
        assert value1 == 'replicated_value', f"Secondary 1: Expected 'replicated_value', got {value1}"
        assert value2 == 'replicated_value', f"Secondary 2: Expected 'replicated_value', got {value2}"
        
        primary_client.close()
        secondary1_client.close()
        secondary2_client.close()
        
        print("[PASSED]")
    
    finally:
        # Cleanup
        for node_id, proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass


def test_leader_election():
    """Test: Kill primary, verify new primary is elected"""
    print("\n[Cluster Test 2] Leader Election: Kill primary, verify new leader")
    
    cleanup_files('cluster_nodes.json',
                   'kvstore_node1.db', 'kvstore_node1.wal',
                   'kvstore_node2.db', 'kvstore_node2.wal',
                   'kvstore_node3.db', 'kvstore_node3.wal')
    
    nodes_config = create_nodes_config()
    
    processes = []
    for node in nodes_config:
        proc = subprocess.Popen(
            ['python', '-m', 'distributed.cluster.cluster_server',
             '--node-id', node['id'],
             '--host', node['host'],
             '--port', str(node['port']),
             '--nodes', 'cluster_nodes.json'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append((node['id'], proc))
    
    try:
        # Wait for all servers
        for node in nodes_config:
            url = f"http://{node['host']}:{node['port']}/health"
            if not wait_for_server(url, timeout=15):
                raise Exception(f"Server {node['id']} failed to start")
        
        time.sleep(2)
        
        # Write some data to primary
        primary_client = KVStoreClient(host='127.0.0.1', port=5001)
        primary_client.set('election_test_key', 'election_test_value')
        primary_client.close()
        
        # Kill primary
        primary_proc = next(proc for node_id, proc in processes if node_id == 'node1')
        if os.name == 'nt':
            primary_proc.kill()
        else:
            primary_proc.send_signal(signal.SIGKILL)
        primary_proc.wait()
        
        processes = [(node_id, proc) for node_id, proc in processes if node_id != 'node1']
        
        time.sleep(5)  # Wait for election
        
        # Check which node is now primary
        new_primary_found = False
        for node_id, proc in processes:
            try:
                response = requests.get(f"http://127.0.0.1:{5001 + int(node_id[-1])}/cluster/status", timeout=2)
                if response.status_code == 200:
                    status = response.json()
                    if status.get('primary_node') == node_id:
                        new_primary_found = True
                        
                        # Verify we can write to new primary
                        new_client = KVStoreClient(host='127.0.0.1', port=5001 + int(node_id[-1]))
                        new_client.set('election_test_key2', 'election_test_value2')
                        value = new_client.get('election_test_key2')
                        assert value == 'election_test_value2'
                        new_client.close()
                        break
            except:
                continue
        
        assert new_primary_found, "New primary was not elected"
        print("[PASSED]")
    
    finally:
        for node_id, proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass


def test_cluster_persistence():
    """Test: Data persists after cluster restart"""
    print("\n[Cluster Test 3] Persistence: Data survives cluster restart")
    
    cleanup_files('cluster_nodes.json',
                   'kvstore_node1.db', 'kvstore_node1.wal',
                   'kvstore_node2.db', 'kvstore_node2.wal',
                   'kvstore_node3.db', 'kvstore_node3.wal')
    
    nodes_config = create_nodes_config()
    
    # First run
    processes = []
    for node in nodes_config:
        proc = subprocess.Popen(
            ['python', '-m', 'distributed.cluster.cluster_server',
             '--node-id', node['id'],
             '--host', node['host'],
             '--port', str(node['port']),
             '--nodes', 'cluster_nodes.json'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append((node['id'], proc))
    
    try:
        for node in nodes_config:
            url = f"http://{node['host']}:{node['port']}/health"
            if not wait_for_server(url, timeout=15):
                raise Exception(f"Server {node['id']} failed to start")
        
        time.sleep(2)
        
        # Write data
        client = KVStoreClient(host='127.0.0.1', port=5001)
        client.set('persist_key', 'persist_value')
        client.checkpoint()
        client.close()
        
    finally:
        for node_id, proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass
    
    time.sleep(1)
    
    # Second run (restart)
    processes = []
    for node in nodes_config:
        proc = subprocess.Popen(
            ['python', 'cluster_server.py',
             '--node-id', node['id'],
             '--host', node['host'],
             '--port', str(node['port']),
             '--nodes', 'cluster_nodes.json'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append((node['id'], proc))
    
    try:
        for node in nodes_config:
            url = f"http://{node['host']}:{node['port']}/health"
            if not wait_for_server(url, timeout=15):
                raise Exception(f"Server {node['id']} failed to start")
        
        time.sleep(2)
        
        # Verify data persisted
        client = KVStoreClient(host='127.0.0.1', port=5001)
        value = client.get('persist_key')
        assert value == 'persist_value', f"Expected 'persist_value', got {value}"
        client.close()
        
        print("[PASSED]")
    
    finally:
        for node_id, proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass


def run_all_cluster_tests():
    """Run all cluster tests"""
    print("=" * 60)
    print("Running Cluster Replication Tests")
    print("=" * 60)
    
    tests = [
        test_cluster_replication,
        test_leader_election,
        test_cluster_persistence,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"[FAILED] {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Cluster tests completed: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_cluster_tests()
    exit(0 if success else 1)
