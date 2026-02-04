# Key-Value Store

A high-performance, persistent key-value store built on top of TCP (HTTP) with support for replication, indexing, and advanced features.

## Features

- **Basic Operations**: Set, Get, Delete, BulkSet
- **Persistence**: Write-Ahead Logging (WAL) for durability
- **HTTP API**: RESTful interface built on Flask
- **Client Library**: Easy-to-use Python client
- **Tests**: Comprehensive test suite
- **Benchmarks**: Write throughput and durability benchmarks
- **ACID Compliance**: Atomic operations with proper locking
- **Debug Mode**: Simulate file system sync issues

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

### Start the Server

```bash
python server.py
```

Server will start on `http://127.0.0.1:5000`

### Using the Client

```python
from client import KVStoreClient

client = KVStoreClient()

# Set a value
client.set("my_key", "my_value")

# Get a value
value = client.get("my_key")

# Delete a key
client.delete("my_key")

# Bulk set
client.bulk_set([("key1", "value1"), ("key2", "value2")])

# Create checkpoint
client.checkpoint()

client.close()
```

## Running Tests

```bash
python tests.py
```

## Running Benchmarks

```bash
# Write throughput benchmark
python benchmarks.py throughput

# Durability benchmark
python benchmarks.py durability

# Bulk write durability
python benchmarks.py bulk_durability

# All benchmarks
python benchmarks.py all
```

## Server Options

```bash
python server.py --help

Options:
  --host HOST        Host to bind to (default: 127.0.0.1)
  --port PORT        Port to bind to (default: 5000)
  --db-path PATH     Database file path (default: kvstore.db)
  --wal-path PATH    WAL file path (default: kvstore.wal)
  --debug            Enable debug mode (simulate sync issues)
```

## Architecture

- **KVStore**: Core in-memory store with persistence
- **Server**: HTTP server using Flask
- **Client**: Python client library
- **WAL**: Write-Ahead Log for durability
- **Snapshots**: Periodic checkpoints to disk

## API Endpoints

- `POST /set` - Set a key-value pair
- `GET /get/<key>` - Get value by key
- `DELETE /delete/<key>` - Delete a key
- `POST /bulk_set` - Bulk set multiple pairs
- `POST /checkpoint` - Create checkpoint
- `GET /health` - Health check

## Advanced Features

### ✅ Cluster Replication (Primary-Secondary)
- Primary node handles all writes
- Automatic replication to secondaries
- Leader election when primary fails
- See `cluster_server.py` and `cluster_tests.py`

### ✅ Master-less Replication
- All nodes can accept reads and writes
- Vector clocks for conflict resolution
- Eventual and strong consistency options
- See `masterless_server.py`

### ✅ Indexing
- Full-text search using inverted index
- Word embedding similarity search
- See `indexes.py` and examples

## Project Structure

```
kv_store/
├── kv_store.py          # Core KV store implementation
├── server.py             # HTTP server (single node)
├── client.py             # Python client library
├── tests.py              # Comprehensive tests
├── benchmarks.py         # Performance benchmarks
├── cluster.py            # Cluster replication logic
├── cluster_server.py     # Cluster-aware server
├── cluster_tests.py      # Cluster replication tests
├── masterless.py         # Master-less replication logic
├── masterless_server.py  # Master-less server
├── indexes.py            # Full-text and embedding indexes
├── example.py            # Usage examples
├── requirements.txt      # Dependencies
└── README.md             # This file
```

## Architecture Details

### Persistence
- **WAL (Write-Ahead Log)**: All writes go to WAL first (synchronous)
- **Snapshots**: Periodic checkpoints to disk
- **Recovery**: Loads from WAL first, then snapshot

### Replication
- **Primary-Secondary**: Writes go to primary, replicated to secondaries
- **Master-less**: All nodes accept writes, conflict resolution via vector clocks

### Indexing
- **Full-text**: Inverted index with TF-IDF scoring
- **Embeddings**: Hash-based embeddings with cosine similarity
