"""
Benchmarks for KV Store.
"""
import os
import time
import threading
import subprocess
import signal
import random
import requests
from core.client import KVStoreClient


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


def benchmark_write_throughput():
    """Benchmark: Write throughput with growing dataset"""
    print("\n" + "=" * 60)
    print("BENCHMARK: Write Throughput")
    print("=" * 60)
    
    cleanup_files("bench_kvstore.db", "bench_kvstore.wal")
    
    server_process = subprocess.Popen(
        ["python", "-m", "core.server", "--db-path", "bench_kvstore.db", "--wal-path", "bench_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    try:
        if not wait_for_server():
            raise Exception("Server failed to start")
        
        client = KVStoreClient()
        
        # Test with different dataset sizes
        for size in [1000, 10000, 50000]:
            print(f"\nPre-populating with {size} keys...")
            
            # Pre-populate
            items = [(f"pre_key_{i}", f"pre_value_{i}") for i in range(size)]
            start_pre = time.time()
            client.bulk_set(items)
            pre_time = time.time() - start_pre
            print(f"Pre-population took {pre_time:.2f} seconds")
            
            # Benchmark writes
            num_writes = 10000
            start = time.time()
            for i in range(num_writes):
                client.set(f"bench_key_{i}", f"bench_value_{i}")
            elapsed = time.time() - start
            
            throughput = num_writes / elapsed
            print(f"Dataset size: {size:>6} | Writes: {num_writes:>6} | "
                  f"Time: {elapsed:>8.2f}s | Throughput: {throughput:>10.2f} writes/sec")
        
        client.close()
    finally:
        server_process.terminate()
        server_process.wait()


def benchmark_durability():
    """Benchmark: Durability test with random crashes"""
    print("\n" + "=" * 60)
    print("BENCHMARK: Durability (Data Loss)")
    print("=" * 60)
    
    cleanup_files("durability_kvstore.db", "durability_kvstore.wal")
    
    server_process = None
    acknowledged_keys = set()
    lost_keys = set()
    lock = threading.Lock()
    
    def write_worker():
        """Thread that writes data and tracks acknowledged writes"""
        nonlocal server_process, acknowledged_keys
        
        client = None
        for i in range(1000):
            try:
                if client is None or not client.health():
                    client = KVStoreClient()
                
                key = f"durable_key_{i}"
                client.set(key, f"value_{i}")
                
                with lock:
                    acknowledged_keys.add(key)
                
                time.sleep(0.001)  # Small delay
            except Exception as e:
                # Server might be down, try to reconnect
                if client:
                    client.close()
                client = None
                time.sleep(0.1)
        
        if client:
            client.close()
    
    def crash_worker():
        """Thread that kills the server randomly"""
        nonlocal server_process
        
        time.sleep(0.5)  # Let some writes happen first
        
        crash_count = 0
        while len(acknowledged_keys) < 500 and crash_count < 10:
            time.sleep(random.uniform(0.2, 0.5))
            
            # Kill server
            if server_process:
                try:
                    # Use SIGKILL equivalent on Windows
                    if os.name == 'nt':
                        server_process.kill()
                    else:
                        server_process.send_signal(signal.SIGKILL)
                    server_process.wait(timeout=2)
                except:
                    pass
            
            time.sleep(0.2)
            
            # Restart server
            server_process = subprocess.Popen(
                ["python", "-m", "core.server", "--db-path", "durability_kvstore.db",
                 "--wal-path", "durability_kvstore.wal"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            if not wait_for_server():
                print("Warning: Server restart failed")
            
            crash_count += 1
    
    # Start initial server
    server_process = subprocess.Popen(
        ["python", "-m", "core.server", "--db-path", "durability_kvstore.db",
         "--wal-path", "durability_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    if not wait_for_server():
        print("Error: Server failed to start")
        return
    
    write_thread = threading.Thread(target=write_worker)
    crash_thread = threading.Thread(target=crash_worker)
    
    write_thread.start()
    crash_thread.start()
    
    write_thread.join()
    crash_thread.join()
    
    # Final checkpoint
    if server_process:
        try:
            client = KVStoreClient()
            client.checkpoint()
            client.close()
        except:
            pass
    
    time.sleep(1)
    
    # Restart server one more time to check durability
    if server_process:
        try:
            server_process.terminate()
            server_process.wait()
        except:
            pass
    
    server_process = subprocess.Popen(
        ["python", "server.py", "--db-path", "durability_kvstore.db", 
         "--wal-path", "durability_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    if wait_for_server():
        # Check which keys were lost
        client = KVStoreClient()
        for key in acknowledged_keys:
            try:
                result = client.get(key)
                if result is None:
                    lost_keys.add(key)
            except:
                lost_keys.add(key)
        client.close()
    
    # Calculate loss rate
    loss_rate = len(lost_keys) / len(acknowledged_keys) if acknowledged_keys else 0
    
    print(f"\nAcknowledged writes: {len(acknowledged_keys)}")
    print(f"Lost keys: {len(lost_keys)}")
    print(f"Data loss rate: {loss_rate * 100:.2f}%")
    
    if server_process:
        server_process.terminate()
        server_process.wait()


def benchmark_bulk_durability():
    """Benchmark: Bulk writes with random crashes"""
    print("\n" + "=" * 60)
    print("BENCHMARK: Bulk Write Durability")
    print("=" * 60)
    
    cleanup_files("bulk_durability_kvstore.db", "bulk_durability_kvstore.wal")
    
    server_process = None
    acknowledged_batches = []
    lock = threading.Lock()
    
    def bulk_write_worker():
        """Thread that does bulk writes"""
        nonlocal server_process, acknowledged_batches
        
        client = None
        batch_id = 0
        
        for _ in range(100):
            try:
                if client is None or not client.health():
                    client = KVStoreClient()
                
                # Create a batch of 50 items
                items = [(f"bulk_key_{batch_id}_{i}", f"bulk_value_{batch_id}_{i}") 
                         for i in range(50)]
                
                client.bulk_set(items)
                
                with lock:
                    acknowledged_batches.append(batch_id)
                
                batch_id += 1
                time.sleep(0.01)
            except Exception as e:
                if client:
                    client.close()
                client = None
                time.sleep(0.1)
        
        if client:
            client.close()
    
    def crash_worker():
        """Thread that kills server randomly"""
        nonlocal server_process
        
        time.sleep(0.5)
        crash_count = 0
        
        while len(acknowledged_batches) < 50 and crash_count < 5:
            time.sleep(random.uniform(0.3, 0.6))
            
            if server_process:
                try:
                    if os.name == 'nt':
                        server_process.kill()
                    else:
                        server_process.send_signal(signal.SIGKILL)
                    server_process.wait(timeout=2)
                except:
                    pass
            
            time.sleep(0.2)
            
            server_process = subprocess.Popen(
                ["python", "-m", "core.server", "--db-path", "bulk_durability_kvstore.db",
                 "--wal-path", "bulk_durability_kvstore.wal"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            if not wait_for_server():
                print("Warning: Server restart failed")
            
            crash_count += 1
    
    server_process = subprocess.Popen(
        ["python", "-m", "core.server", "--db-path", "bulk_durability_kvstore.db",
         "--wal-path", "bulk_durability_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    if not wait_for_server():
        print("Error: Server failed to start")
        return
    
    write_thread = threading.Thread(target=bulk_write_worker)
    crash_thread = threading.Thread(target=crash_worker)
    
    write_thread.start()
    crash_thread.start()
    
    write_thread.join()
    crash_thread.join()
    
    # Final checkpoint
    if server_process:
        try:
            client = KVStoreClient()
            client.checkpoint()
            client.close()
        except:
            pass
    
    time.sleep(1)
    
    # Restart and check
    if server_process:
        try:
            server_process.terminate()
            server_process.wait()
        except:
            pass
    
    server_process = subprocess.Popen(
        ["python", "server.py", "--db-path", "bulk_durability_kvstore.db", 
         "--wal-path", "bulk_durability_kvstore.wal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    complete_batches = 0
    partial_batches = 0
    
    if wait_for_server():
        client = KVStoreClient()
        for batch_id in acknowledged_batches:
            complete = True
            found_any = False
            
            for i in range(50):
                key = f"bulk_key_{batch_id}_{i}"
                value = client.get(key)
                if value:
                    found_any = True
                else:
                    complete = False
            
            if complete:
                complete_batches += 1
            elif found_any:
                partial_batches += 1
        
        client.close()
    
    print(f"\nAcknowledged batches: {len(acknowledged_batches)}")
    print(f"Complete batches: {complete_batches}")
    print(f"Partial batches: {partial_batches}")
    print(f"Lost batches: {len(acknowledged_batches) - complete_batches - partial_batches}")
    
    if server_process:
        server_process.terminate()
        server_process.wait()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "throughput":
            benchmark_write_throughput()
        elif sys.argv[1] == "durability":
            benchmark_durability()
        elif sys.argv[1] == "bulk_durability":
            benchmark_bulk_durability()
        elif sys.argv[1] == "all":
            benchmark_write_throughput()
            benchmark_durability()
            benchmark_bulk_durability()
        else:
            print("Usage: python benchmarks.py [throughput|durability|bulk_durability|all]")
    else:
        benchmark_write_throughput()
        benchmark_durability()
        benchmark_bulk_durability()
