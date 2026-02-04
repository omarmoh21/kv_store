"""
Example client script for KV Store
Make sure the server is running: python server.py
"""
from client import KVStoreClient


def main():
    # Create client (connects to server at 127.0.0.1:5000)
    client = KVStoreClient()
    
    # Check if server is running
    if not client.health():
        print("Error: Server is not running!")
        print("Start it with: python server.py")
        return
    
    print("Connected to KV Store server!")
    print("-" * 40)
    
    # Set some values
    print("\n1. Setting values...")
    client.set("name", "Alice")
    client.set("age", 30)
    client.set("city", "New York")
    print("   ✓ Set: name = Alice")
    print("   ✓ Set: age = 30")
    print("   ✓ Set: city = New York")
    
    # Get values
    print("\n2. Getting values...")
    print(f"   name = {client.get('name')}")
    print(f"   age = {client.get('age')}")
    print(f"   city = {client.get('city')}")
    
    # Bulk set
    print("\n3. Bulk setting multiple pairs...")
    client.bulk_set([
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3")
    ])
    print("   ✓ Bulk set 3 key-value pairs")
    
    # Get bulk values
    print("\n4. Getting bulk values...")
    print(f"   key1 = {client.get('key1')}")
    print(f"   key2 = {client.get('key2')}")
    print(f"   key3 = {client.get('key3')}")
    
    # Delete a key
    print("\n5. Deleting a key...")
    client.delete("age")
    age_after_delete = client.get("age")
    print(f"   ✓ Deleted 'age', value now: {age_after_delete}")
    
    # Try to get non-existent key
    print("\n6. Getting non-existent key...")
    result = client.get("nonexistent")
    print(f"   nonexistent = {result}")
    
    # Create checkpoint
    print("\n7. Creating checkpoint...")
    client.checkpoint()
    print("   ✓ Checkpoint created")
    
    print("\n" + "-" * 40)
    print("All operations completed successfully!")
    
    # Close client
    client.close()


if __name__ == "__main__":
    main()
