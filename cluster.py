"""
Cluster Replication System
Primary-Secondary architecture with leader election
"""
import json
import os
import threading
import time
import requests
import random
from typing import List, Dict, Optional, Tuple
from kv_store import KVStore
from client import KVStoreClient


class ClusterNode:
    """Represents a node in the cluster"""
    
    def __init__(self, node_id: str, host: str, port: int, is_primary: bool = False):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.is_primary = is_primary
        self.is_alive = True
        self.last_heartbeat = time.time()
        self.store = None
    
    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
    
    def health_check(self) -> bool:
        """Check if node is healthy"""
        try:
            response = requests.get(f"{self.url}/health", timeout=1)
            return response.status_code == 200
        except:
            return False


class ClusterManager:
    """Manages cluster replication and leader election"""
    
    def __init__(self, nodes: List[ClusterNode], current_node_id: str):
        self.nodes = {node.node_id: node for node in nodes}
        self.current_node_id = current_node_id
        self.current_node = self.nodes[current_node_id]
        self.primary_node_id = None
        self.lock = threading.RLock()
        self.election_in_progress = False
        self.heartbeat_interval = 1.0
        self.election_timeout = 3.0
        
        # Find initial primary
        for node_id, node in self.nodes.items():
            if node.is_primary:
                self.primary_node_id = node_id
                break
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
    
    def _heartbeat_loop(self):
        """Continuously check node health and trigger election if needed"""
        while True:
            try:
                time.sleep(self.heartbeat_interval)
                
                # Check primary health
                if self.primary_node_id:
                    primary = self.nodes[self.primary_node_id]
                    if not primary.health_check():
                        print(f"Primary node {self.primary_node_id} is down!")
                        self._start_election()
                
                # Update node statuses
                for node_id, node in self.nodes.items():
                    node.is_alive = node.health_check()
                    if node.is_alive:
                        node.last_heartbeat = time.time()
            
            except Exception as e:
                print(f"Heartbeat error: {e}")
    
    def _start_election(self):
        """Start leader election"""
        if self.election_in_progress:
            return
        
        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
        
        try:
            print(f"Starting election from node {self.current_node_id}")
            
            # Sort nodes by ID for deterministic election
            sorted_nodes = sorted(self.nodes.items(), key=lambda x: x[0])
            
            # Find first alive node
            for node_id, node in sorted_nodes:
                if node.health_check():
                    print(f"Elected new primary: {node_id}")
                    self.primary_node_id = node_id
                    node.is_primary = True
                    
                    # Update other nodes
                    for other_id, other_node in self.nodes.items():
                        if other_id != node_id:
                            other_node.is_primary = False
                    
                    break
            
        finally:
            with self.lock:
                self.election_in_progress = False
    
    def get_primary(self) -> Optional[ClusterNode]:
        """Get the current primary node"""
        if self.primary_node_id:
            primary = self.nodes[self.primary_node_id]
            if primary.is_alive:
                return primary
        
        # Trigger election if no primary
        self._start_election()
        if self.primary_node_id:
            return self.nodes[self.primary_node_id]
        return None
    
    def replicate_to_secondaries(self, operation: Dict):
        """Replicate operation to all secondary nodes"""
        secondaries = [node for node_id, node in self.nodes.items() 
                      if node_id != self.primary_node_id and node.is_alive]
        
        for secondary in secondaries:
            try:
                if operation['cmd'] == 'set':
                    client = KVStoreClient(host=secondary.host, port=secondary.port)
                    client.set(operation['key'], operation['value'])
                    client.close()
                elif operation['cmd'] == 'delete':
                    client = KVStoreClient(host=secondary.host, port=secondary.port)
                    client.delete(operation['key'])
                    client.close()
                elif operation['cmd'] == 'bulk_set':
                    client = KVStoreClient(host=secondary.host, port=secondary.port)
                    client.bulk_set(operation['items'])
                    client.close()
            except Exception as e:
                print(f"Replication error to {secondary.node_id}: {e}")


class ClusterKVStore:
    """KV Store with cluster replication support"""
    
    def __init__(self, cluster_manager: ClusterManager, db_path: str, wal_path: str):
        self.cluster_manager = cluster_manager
        self.db_path = db_path
        self.wal_path = wal_path
        self.store = KVStore(db_path=db_path, wal_path=wal_path)
        self.is_primary = False
    
    def set(self, key: str, value, debug=None):
        """Set key-value pair (only on primary)"""
        primary = self.cluster_manager.get_primary()
        if not primary:
            raise Exception("No primary node available")
        
        if primary.node_id == self.cluster_manager.current_node_id:
            # We are primary, replicate to secondaries
            self.store.set(key, value, debug=debug)
            operation = {'cmd': 'set', 'key': key, 'value': value}
            self.cluster_manager.replicate_to_secondaries(operation)
        else:
            # Forward to primary
            client = KVStoreClient(host=primary.host, port=primary.port)
            client.set(key, value, debug=debug)
            client.close()
        
        return True
    
    def get(self, key: str):
        """Get value (only from primary)"""
        primary = self.cluster_manager.get_primary()
        if not primary:
            raise Exception("No primary node available")
        
        if primary.node_id == self.cluster_manager.current_node_id:
            return self.store.get(key)
        else:
            client = KVStoreClient(host=primary.host, port=primary.port)
            value = client.get(key)
            client.close()
            return value
    
    def delete(self, key: str):
        """Delete key (only on primary)"""
        primary = self.cluster_manager.get_primary()
        if not primary:
            raise Exception("No primary node available")
        
        if primary.node_id == self.cluster_manager.current_node_id:
            self.store.delete(key)
            operation = {'cmd': 'delete', 'key': key}
            self.cluster_manager.replicate_to_secondaries(operation)
        else:
            client = KVStoreClient(host=primary.host, port=primary.port)
            client.delete(key)
            client.close()
        
        return True
    
    def bulk_set(self, items: List[Tuple[str, any]], debug=None):
        """Bulk set (only on primary)"""
        primary = self.cluster_manager.get_primary()
        if not primary:
            raise Exception("No primary node available")
        
        if primary.node_id == self.cluster_manager.current_node_id:
            self.store.bulk_set(items, debug=debug)
            operation = {'cmd': 'bulk_set', 'items': items}
            self.cluster_manager.replicate_to_secondaries(operation)
        else:
            client = KVStoreClient(host=primary.host, port=primary.port)
            client.bulk_set(items, debug=debug)
            client.close()
        
        return True
    
    def checkpoint(self):
        """Create checkpoint"""
        if self.cluster_manager.current_node_id == self.cluster_manager.primary_node_id:
            self.store.checkpoint()
