"""
Test Word Embedding Search
Demonstrates similarity search using word embeddings
"""
from kv_store import KVStore
from indexes import IndexedKVStore


def main():
    print("=" * 60)
    print("Word Embedding Search Demo")
    print("=" * 60)
    
    # Create base store
    base_store = KVStore(db_path="embedding_test.db", wal_path="embedding_test.wal")
    
    # Create indexed store with embeddings enabled
    indexed_store = IndexedKVStore(
        base_store, 
        enable_fulltext=True, 
        enable_embeddings=True
    )
    
    print("\n1. Adding documents to the store...")
    print("-" * 60)
    
    # Add various documents about different topics
    documents = [
        ("doc1", "Python programming language tutorial for beginners"),
        ("doc2", "Machine learning algorithms and neural networks"),
        ("doc3", "Java development best practices and design patterns"),
        ("doc4", "Python web development with Flask and Django"),
        ("doc5", "Deep learning and artificial intelligence research"),
        ("doc6", "JavaScript frontend development and React framework"),
        ("doc7", "Python data science and pandas library"),
        ("doc8", "Natural language processing and text analysis"),
        ("doc9", "Python automation scripts and tools"),
        ("doc10", "Web development with HTML CSS and JavaScript"),
    ]
    
    for key, value in documents:
        indexed_store.set(key, value)
        print(f"   [OK] Added: {key}")
    
    print("\n2. Testing Word Embedding Similarity Search")
    print("-" * 60)
    
    # Test queries
    queries = [
        "Python programming",
        "machine learning",
        "web development",
        "data science",
        "artificial intelligence"
    ]
    
    for query in queries:
        print(f"\n   Query: '{query}'")
        print("   " + "-" * 50)
        
        # Search using embeddings (similarity)
        results = indexed_store.search_similar(query, limit=5)
        
        if results:
            print("   Top similar documents:")
            for i, (key, score) in enumerate(results, 1):
                value = indexed_store.get(key)
                print(f"   {i}. {key} (similarity: {score:.4f})")
                print(f"      Content: {value}")
        else:
            print("   No results found")
    
    print("\n3. Comparing with Full-Text Search")
    print("-" * 60)
    
    query = "Python"
    print(f"\n   Query: '{query}'")
    
    print("\n   Full-text search results:")
    ft_results = indexed_store.search_fulltext(query, limit=5)
    for i, (key, score) in enumerate(ft_results, 1):
        value = indexed_store.get(key)
        print(f"   {i}. {key} (TF-IDF score: {score:.4f})")
        print(f"      Content: {value}")
    
    print("\n   Embedding similarity search results:")
    emb_results = indexed_store.search_similar(query, limit=5)
    for i, (key, score) in enumerate(emb_results, 1):
        value = indexed_store.get(key)
        print(f"   {i}. {key} (similarity: {score:.4f})")
        print(f"      Content: {value}")
    
    print("\n4. Testing with partial/misspelled queries")
    print("-" * 60)
    
    partial_queries = [
        "progrming",  # misspelled
        "machin",     # partial
        "web dev",    # abbreviated
    ]
    
    for query in partial_queries:
        print(f"\n   Query: '{query}'")
        results = indexed_store.search_similar(query, limit=3)
        if results:
            for key, score in results:
                print(f"   - {key} (similarity: {score:.4f})")
    
    print("\n" + "=" * 60)
    print("Embedding search demo completed!")
    print("=" * 60)
    
    # Cleanup
    indexed_store.checkpoint()


if __name__ == "__main__":
    main()
