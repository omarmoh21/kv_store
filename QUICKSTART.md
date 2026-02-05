# Quick Start Guide

## Installation

```bash
pip install -r requirements.txt
```

## Basic Usage

### 1. Single Node Server

```bash
# Start server
python -m core.server

# In another terminal, use the client
python -c "from core.client import KVStoreClient; c = KVStoreClient(); c.set('key', 'value'); print(c.get('key'))"
```

### 2. Run Tests

```bash
python -m tests.tests
```

### 3. Run Benchmarks

```bash
# Write throughput
python -m benchmarks.benchmarks throughput

# Durability
python -m benchmarks.benchmarks durability

# All benchmarks
python -m benchmarks.benchmarks all
```

## Cluster Replication (Primary-Secondary)

### Setup

1. Create `cluster_nodes.json`:
```json
[
  {"id": "node1", "host": "127.0.0.1", "port": 5001, "is_primary": true},
  {"id": "node2", "host": "127.0.0.1", "port": 5002, "is_primary": false},
  {"id": "node3", "host": "127.0.0.1", "port": 5003, "is_primary": false}
]
```

2. Start nodes:
```bash
# Terminal 1
python -m distributed.cluster.cluster_server --node-id node1 --port 5001 --nodes cluster_nodes.json

# Terminal 2
python -m distributed.cluster.cluster_server --node-id node2 --port 5002 --nodes cluster_nodes.json

# Terminal 3
python -m distributed.cluster.cluster_server --node-id node3 --port 5003 --nodes cluster_nodes.json
```

3. Run cluster tests:
```bash
python -m tests.cluster_tests
```

## Master-less Replication

### Setup

1. Create `masterless_nodes.json`:
```json
[
  {"id": "node1", "host": "127.0.0.1", "port": 6001},
  {"id": "node2", "host": "127.0.0.1", "port": 6002},
  {"id": "node3", "host": "127.0.0.1", "port": 6003}
]
```

2. Start nodes:
```bash
# Terminal 1
python -m distributed.masterless.masterless_server --node-id node1 --port 6001 --nodes masterless_nodes.json

# Terminal 2
python -m distributed.masterless.masterless_server --node-id node2 --port 6002 --nodes masterless_nodes.json

# Terminal 3
python -m distributed.masterless.masterless_server --node-id node3 --port 6003 --nodes masterless_nodes.json
```

3. Use any node for reads/writes:
```python
from client import KVStoreClient

# Write to any node
client = KVStoreClient(host="127.0.0.1", port=6001)
client.set("key", "value")

# Read from any node (eventual consistency)
value = client.get("key")

# Strong consistency read
value = client.get("key", params={"consistency": "strong"})
```

## Indexed Store

```python
from core.kv_store import KVStore
from core.indexes import IndexedKVStore

# Create indexed store
base_store = KVStore(db_path="indexed.db", wal_path="indexed.wal")
indexed_store = IndexedKVStore(base_store, enable_fulltext=True, enable_embeddings=True)

# Add documents
indexed_store.set("doc1", "Python programming tutorial")
indexed_store.set("doc2", "Java development guide")

# Full-text search
results = indexed_store.search_fulltext("Python", limit=10)
for key, score in results:
    print(f"{key}: {score}")

# Similarity search
results = indexed_store.search_similar("programming", limit=10)
for key, score in results:
    print(f"{key}: {score}")
```

## Examples

Examples can be created using the `core.client` and `core.kv_store` modules as shown above.
