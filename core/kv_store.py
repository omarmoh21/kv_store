"""
Key-Value Store with Persistence
Supports Set, Get, Delete, BulkSet operations
Uses Write-Ahead Logging (WAL) for durability
"""
import json
import os
import threading
import random
import time
from pathlib import Path
from collections import defaultdict


class KVStore:
    """In-memory key-value store with persistence"""
    
    def __init__(self, db_path="kvstore.db", wal_path="kvstore.wal", debug=False):
        self.db_path = db_path
        self.wal_path = wal_path
        self.data = {}
        self.lock = threading.RLock()  # Reentrant lock for nested calls
        self.debug = debug  # For simulating file system sync issues
        
        # Ensure directories exist
        self._ensure_paths()
        
        # Load data from disk
        self._load_from_disk()
    
    def _ensure_paths(self):
        """Ensure data directory exists"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.wal_path).parent.mkdir(parents=True, exist_ok=True)
    
    def _load_from_disk(self):
        """Load data from WAL first, then from main DB"""
        # Load from WAL (has latest committed operations)
        if os.path.exists(self.wal_path):
            try:
                with open(self.wal_path, 'r') as f:
                    for line in f:
                        if line.strip():
                            try:
                                op = json.loads(line.strip())
                                self._apply_op(op)
                            except json.JSONDecodeError:
                                continue
            except Exception as e:
                print(f"Error loading WAL: {e}")
        
        # Load from main DB snapshot
        if os.path.exists(self.db_path):
            try:
                with open(self.db_path, 'r') as f:
                    self.data = json.load(f)
            except Exception as e:
                print(f"Error loading DB: {e}")
                self.data = {}
    
    def _apply_op(self, op):
        """Apply operation to in-memory store"""
        cmd = op.get('cmd')
        if cmd == 'set':
            self.data[op['key']] = op['value']
        elif cmd == 'delete':
            self.data.pop(op['key'], None)
        elif cmd == 'bulk_set':
            for key, value in op.get('items', []):
                self.data[key] = value
    
    def _write_to_wal(self, op):
        """Write operation to WAL synchronously (always succeeds)"""
        with open(self.wal_path, 'a') as f:
            f.write(json.dumps(op) + '\n')
            f.flush()
            os.fsync(f.fileno())  # Force sync to disk
    
    def _save_snapshot(self):
        """Save entire dataset to disk (checkpoint)"""
        if self.debug:
            # Simulate file system sync issues (1% chance)
            if random.random() < 0.01:
                return
        
        try:
            with open(self.db_path, 'w') as f:
                json.dump(self.data, f)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            print(f"Error saving snapshot: {e}")
    
    def set(self, key, value, debug=None):
        """Set key-value pair"""
        debug = debug if debug is not None else self.debug
        op = {'cmd': 'set', 'key': key, 'value': value}
        
        with self.lock:
            # Write to WAL first (always synchronous)
            self._write_to_wal(op)
            # Apply to in-memory store
            self._apply_op(op)
        
        return True
    
    def get(self, key):
        """Get value by key"""
        with self.lock:
            return self.data.get(key)
    
    def delete(self, key):
        """Delete key"""
        op = {'cmd': 'delete', 'key': key}
        
        with self.lock:
            # Write to WAL first
            self._write_to_wal(op)
            # Apply to in-memory store
            self._apply_op(op)
        
        return True
    
    def bulk_set(self, items, debug=None):
        """Bulk set multiple key-value pairs atomically"""
        debug = debug if debug is not None else self.debug
        op = {'cmd': 'bulk_set', 'items': items}
        
        with self.lock:
            # Write to WAL first
            self._write_to_wal(op)
            # Apply to in-memory store
            self._apply_op(op)
        
        return True
    
    def checkpoint(self):
        """Create checkpoint: save snapshot and clear WAL"""
        with self.lock:
            self._save_snapshot()
            if os.path.exists(self.wal_path):
                os.remove(self.wal_path)
    
    def get_all_keys(self):
        """Get all keys (for testing/debugging)"""
        with self.lock:
            return list(self.data.keys())
