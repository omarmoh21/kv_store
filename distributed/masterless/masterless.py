"""
Master-less Replication System.
All nodes can accept reads and writes.
Uses vector clocks for conflict resolution.
"""
import json
import time
import threading
import requests
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict

from core.kv_store import KVStore


class VectorClock:
    """Vector clock for tracking causality"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock: Dict[str, int] = {node_id: 0}
    
    def tick(self):
        """Increment own clock"""
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1
    
    def update(self, other_clock: Dict[str, int]):
        """Update clock with another clock (merge)"""
        for node_id, timestamp in other_clock.items():
            self.clock[node_id] = max(
                self.clock.get(node_id, 0),
                timestamp
            )
        # Increment own clock
        self.tick()
    
    def compare(self, other_clock: Dict[str, int]) -> str:
        """
        Compare two vector clocks
        Returns: 'before', 'after', 'concurrent', 'equal'
        """
        self_keys = set(self.clock.keys())
        other_keys = set(other_clock.keys())
        all_keys = self_keys | other_keys
        
        self_greater = False
        other_greater = False
        
        for key in all_keys:
            self_val = self.clock.get(key, 0)
            other_val = other_clock.get(key, 0)
            
            if self_val > other_val:
                self_greater = True
            elif other_val > self_val:
                other_greater = True
        
        if self_greater and not other_greater:
            return 'after'
        elif other_greater and not self_greater:
            return 'before'
        elif not self_greater and not other_greater:
            return 'equal'
        else:
            return 'concurrent'
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary"""
        return self.clock.copy()
    
    def from_dict(self, clock_dict: Dict[str, int]):
        """Load from dictionary"""
        self.clock = clock_dict.copy()


class MasterlessNode:
    """A node in master-less cluster"""
    
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.is_alive = True
        self.last_heartbeat = time.time()
    
    @property
    def url(self):
        return f"http://{self.host}:{self.port}"


class MasterlessKVStore:
    """KV Store with master-less replication"""
    
    def __init__(self, node_id: str, nodes: List[MasterlessNode], 
                 db_path: str, wal_path: str, replication_factor: int = 2):
        self.node_id = node_id
        self.nodes = {node.node_id: node for node in nodes}
        self.replication_factor = replication_factor
        self.store = KVStore(db_path=db_path, wal_path=wal_path)
        
        # Vector clock for this node
        self.vector_clock = VectorClock(node_id)
        
        # Store versioned values: key -> (value, vector_clock)
        self.versioned_data: Dict[str, Tuple[any, Dict[str, int]]] = {}
        
        # Load versioned data from store
        self._load_versioned_data()
        
        self.lock = threading.RLock()
        self.replication_thread = None
        self.running = True
        
        # Start replication thread
        self._start_replication()
    
    def _load_versioned_data(self):
        """Load versioned data from store"""
        # For simplicity, we'll store version info separately
        # In production, this would be persisted
        pass
    
    def _start_replication(self):
        """Start background replication thread"""
        def replicate_loop():
            while self.running:
                try:
                    time.sleep(1)  # Replicate every second
                    self._replicate_to_peers()
                except Exception as e:
                    print(f"Replication error: {e}")
        
        self.replication_thread = threading.Thread(target=replicate_loop, daemon=True)
        self.replication_thread.start()
    
    def _replicate_to_peers(self):
        """Replicate data to other nodes"""
        # Get other nodes (excluding self)
        other_nodes = [node for node_id, node in self.nodes.items() 
                      if node_id != self.node_id and node.is_alive]
        
        if not other_nodes:
            return
        
        # Select nodes for replication (up to replication_factor)
        nodes_to_replicate = other_nodes[:self.replication_factor]
        
        with self.lock:
            # Get all our data
            our_data = {}
            for key in self.store.get_all_keys():
                value = self.store.get(key)
                if value is not None:
                    our_data[key] = {
                        'value': value,
                        'clock': self.vector_clock.to_dict()
                    }
        
        # Replicate to selected nodes
        for node in nodes_to_replicate:
            try:
                self._send_data_to_node(node, our_data)
            except Exception as e:
                print(f"Failed to replicate to {node.node_id}: {e}")
    
    def _send_data_to_node(self, node: MasterlessNode, data: Dict):
        """Send data to a specific node"""
        try:
            response = requests.post(
                f"{node.url}/masterless/replicate",
                json={'node_id': self.node_id, 'data': data},
                timeout=2
            )
            return response.status_code == 200
        except:
            return False
    
    def _resolve_conflict(self, key: str, value1: any, clock1: Dict[str, int],
                         value2: any, clock2: Dict[str, int]) -> Tuple[any, Dict[str, int]]:
        """Resolve conflict between two versions"""
        vc1 = VectorClock(self.node_id)
        vc1.from_dict(clock1)
        
        vc2 = VectorClock(self.node_id)
        vc2.from_dict(clock2)
        
        comparison = vc1.compare(clock2)
        
        if comparison == 'after':
            return value1, clock1
        elif comparison == 'before':
            return value2, clock2
        elif comparison == 'equal':
            # Same version, return either
            return value1, clock1
        else:
            # Concurrent writes - use last-write-wins with node ID tiebreaker
            # In production, you might merge values or use application-specific logic
            if self.node_id in clock1 and self.node_id in clock2:
                if clock1[self.node_id] >= clock2[self.node_id]:
                    return value1, clock1
                else:
                    return value2, clock2
            else:
                # Use lexicographic comparison of node IDs as tiebreaker
                max_node1 = max(clock1.keys()) if clock1 else ''
                max_node2 = max(clock2.keys()) if clock2 else ''
                if max_node1 >= max_node2:
                    return value1, clock1
                else:
                    return value2, clock2
    
    def set(self, key: str, value, debug=None):
        """Set key-value pair (writes to local and replicates)"""
        with self.lock:
            # Update vector clock
            self.vector_clock.tick()
            
            # Store value with version
            self.store.set(key, value, debug=debug)
            self.versioned_data[key] = (value, self.vector_clock.to_dict())
        
        # Replication happens asynchronously
        return True
    
    def get(self, key: str, read_consistency: str = 'eventual'):
        """
        Get value by key
        read_consistency: 'eventual' (fast) or 'strong' (slower, reads from quorum)
        """
        if read_consistency == 'eventual':
            return self.store.get(key)
        else:
            # Strong consistency: read from quorum
            return self._read_from_quorum(key)
    
    def _read_from_quorum(self, key: str):
        """Read from quorum of nodes"""
        quorum_size = (len(self.nodes) // 2) + 1
        
        # Read from self
        local_value = self.store.get(key)
        local_clock = self.versioned_data.get(key, (None, {}))[1]
        
        # Read from other nodes
        other_values = []
        for node_id, node in self.nodes.items():
            if node_id != self.node_id and node.is_alive:
                try:
                    response = requests.get(
                        f"{node.url}/masterless/get/{key}",
                        timeout=1
                    )
                    if response.status_code == 200:
                        data = response.json()
                        other_values.append((data.get('value'), data.get('clock', {})))
                except:
                    pass
        
        # Find latest version
        all_values = [(local_value, local_clock)] + other_values
        latest_value = None
        latest_clock = {}
        
        for value, clock in all_values:
            if value is not None:
                if not latest_clock:
                    latest_value, latest_clock = value, clock
                else:
                    vc = VectorClock(self.node_id)
                    vc.from_dict(latest_clock)
                    comparison = vc.compare(clock)
                    if comparison in ('before', 'concurrent'):
                        latest_value, latest_clock = value, clock
        
        return latest_value
    
    def delete(self, key: str):
        """Delete key"""
        with self.lock:
            self.vector_clock.tick()
            self.store.delete(key)
            self.versioned_data.pop(key, None)
        return True
    
    def bulk_set(self, items: List[Tuple[str, any]], debug=None):
        """Bulk set"""
        with self.lock:
            self.vector_clock.tick()
            self.store.bulk_set(items, debug=debug)
            for key, value in items:
                self.versioned_data[key] = (value, self.vector_clock.to_dict())
        return True
    
    def receive_replication(self, source_node_id: str, data: Dict):
        """Receive replicated data from another node"""
        with self.lock:
            for key, item in data.items():
                value = item.get('value')
                clock = item.get('clock', {})
                
                if key in self.versioned_data:
                    # Conflict resolution
                    current_value, current_clock = self.versioned_data[key]
                    resolved_value, resolved_clock = self._resolve_conflict(
                        key, current_value, current_clock, value, clock
                    )
                    
                    # Update if resolved version is newer
                    vc = VectorClock(self.node_id)
                    vc.from_dict(current_clock)
                    comparison = vc.compare(resolved_clock)
                    
                    if comparison in ('before', 'concurrent'):
                        self.store.set(key, resolved_value)
                        self.versioned_data[key] = (resolved_value, resolved_clock)
                        self.vector_clock.update(resolved_clock)
                else:
                    # New key, just add it
                    self.store.set(key, value)
                    self.versioned_data[key] = (value, clock)
                    self.vector_clock.update(clock)
    
    def checkpoint(self):
        """Create checkpoint"""
        self.store.checkpoint()
    
    def stop(self):
        """Stop replication"""
        self.running = False
