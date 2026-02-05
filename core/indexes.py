"""
Indexing System for KV Store
- Full-text search index
- Word embedding index
"""
import json
import re
import math
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import threading


class FullTextIndex:
    """Full-text search index using inverted index"""
    
    def __init__(self):
        self.inverted_index: Dict[str, Set[str]] = defaultdict(set)  # word -> set of keys
        self.key_texts: Dict[str, str] = {}  # key -> full text
        self.lock = threading.RLock()
    
    def index_value(self, key: str, value: any):
        """Index a value (extract text and build index)"""
        with self.lock:
            # Remove old index entry if exists
            if key in self.key_texts:
                old_text = self.key_texts[key]
                words = self._tokenize(old_text)
                for word in words:
                    self.inverted_index[word].discard(key)
                    if not self.inverted_index[word]:
                        del self.inverted_index[word]
            
            # Convert value to text
            text = self._value_to_text(value)
            self.key_texts[key] = text
            
            # Tokenize and index
            words = self._tokenize(text)
            for word in words:
                self.inverted_index[word].add(key)
    
    def remove_key(self, key: str):
        """Remove a key from index"""
        with self.lock:
            if key in self.key_texts:
                text = self.key_texts[key]
                words = self._tokenize(text)
                for word in words:
                    self.inverted_index[word].discard(key)
                    if not self.inverted_index[word]:
                        del self.inverted_index[word]
                del self.key_texts[key]
    
    def search(self, query: str, limit: int = 10) -> List[Tuple[str, float]]:
        """Search for keys matching query"""
        with self.lock:
            query_words = self._tokenize(query)
            if not query_words:
                return []
            
            # Find keys that contain all query words (AND search)
            matching_keys = None
            for word in query_words:
                keys_with_word = self.inverted_index.get(word, set())
                if matching_keys is None:
                    matching_keys = keys_with_word.copy()
                else:
                    matching_keys &= keys_with_word
            
            if matching_keys is None or not matching_keys:
                return []
            
            # Score results using TF-IDF
            results = []
            for key in matching_keys:
                score = self._calculate_score(key, query_words)
                results.append((key, score))
            
            # Sort by score and return top results
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:limit]
    
    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into words"""
        # Convert to lowercase and split on non-word characters
        words = re.findall(r'\b\w+\b', text.lower())
        # Filter out very short words
        return [w for w in words if len(w) > 2]
    
    def _value_to_text(self, value: any) -> str:
        """Convert value to searchable text"""
        if isinstance(value, str):
            return value
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, dict):
            # Extract all string values from dict
            texts = []
            for v in value.values():
                if isinstance(v, str):
                    texts.append(v)
            return ' '.join(texts)
        elif isinstance(value, list):
            # Extract all string values from list
            texts = []
            for v in value:
                if isinstance(v, str):
                    texts.append(v)
            return ' '.join(texts)
        else:
            return str(value)
    
    def _calculate_score(self, key: str, query_words: List[str]) -> float:
        """Calculate TF-IDF score for a key"""
        text = self.key_texts.get(key, '')
        text_words = self._tokenize(text)
        
        if not text_words:
            return 0.0
        
        # Term Frequency (TF)
        tf_scores = {}
        for word in query_words:
            tf_scores[word] = text_words.count(word) / len(text_words)
        
        # Inverse Document Frequency (IDF)
        total_keys = len(self.key_texts)
        idf_scores = {}
        for word in query_words:
            keys_with_word = len(self.inverted_index.get(word, set()))
            if keys_with_word > 0:
                idf_scores[word] = math.log(total_keys / keys_with_word)
            else:
                idf_scores[word] = 0.0
        
        # TF-IDF score
        score = sum(tf_scores.get(word, 0) * idf_scores.get(word, 0) for word in query_words)
        return score


class WordEmbeddingIndex:
    """Word embedding index using simple vector representations"""
    
    def __init__(self, embedding_dim: int = 128):
        self.embedding_dim = embedding_dim
        self.word_vectors: Dict[str, List[float]] = {}
        self.key_vectors: Dict[str, List[float]] = {}
        self.lock = threading.RLock()
    
    def _hash_to_vector(self, text: str) -> List[float]:
        """Convert text to vector using simple hash-based embedding"""
        vector = [0.0] * self.embedding_dim
        
        # Simple hash-based embedding
        for i, char in enumerate(text):
            hash_val = hash(char + str(i)) % self.embedding_dim
            vector[hash_val] += 1.0
        
        # Normalize
        magnitude = math.sqrt(sum(x * x for x in vector))
        if magnitude > 0:
            vector = [x / magnitude for x in vector]
        
        return vector
    
    def index_value(self, key: str, value: any):
        """Index a value by creating embedding"""
        with self.lock:
            text = self._value_to_text(value)
            vector = self._hash_to_vector(text)
            self.key_vectors[key] = vector
    
    def remove_key(self, key: str):
        """Remove a key from index"""
        with self.lock:
            self.key_vectors.pop(key, None)
    
    def search(self, query: str, limit: int = 10) -> List[Tuple[str, float]]:
        """Search for similar keys using cosine similarity"""
        with self.lock:
            query_vector = self._hash_to_vector(query)
            
            if not self.key_vectors:
                return []
            
            # Calculate cosine similarity
            results = []
            for key, vector in self.key_vectors.items():
                similarity = self._cosine_similarity(query_vector, vector)
                results.append((key, similarity))
            
            # Sort by similarity
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:limit]
    
    def _value_to_text(self, value: any) -> str:
        """Convert value to text"""
        if isinstance(value, str):
            return value
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, dict):
            return ' '.join(str(v) for v in value.values() if isinstance(v, str))
        elif isinstance(value, list):
            return ' '.join(str(v) for v in value if isinstance(v, str))
        else:
            return str(value)
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = math.sqrt(sum(a * a for a in vec1))
        magnitude2 = math.sqrt(sum(a * a for a in vec2))
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)


class IndexedKVStore:
    """KV Store with indexing support"""
    
    def __init__(self, base_store, enable_fulltext: bool = True, enable_embeddings: bool = True):
        self.store = base_store
        self.fulltext_index = FullTextIndex() if enable_fulltext else None
        self.embedding_index = WordEmbeddingIndex() if enable_embeddings else None
    
    def set(self, key: str, value, debug=None):
        """Set key-value pair and update indexes"""
        result = self.store.set(key, value, debug=debug)
        
        # Update indexes
        if self.fulltext_index:
            self.fulltext_index.index_value(key, value)
        if self.embedding_index:
            self.embedding_index.index_value(key, value)
        
        return result
    
    def get(self, key: str):
        """Get value by key"""
        return self.store.get(key)
    
    def delete(self, key: str):
        """Delete key and remove from indexes"""
        result = self.store.delete(key)
        
        # Remove from indexes
        if self.fulltext_index:
            self.fulltext_index.remove_key(key)
        if self.embedding_index:
            self.embedding_index.remove_key(key)
        
        return result
    
    def bulk_set(self, items: List[Tuple[str, any]], debug=None):
        """Bulk set and update indexes"""
        result = self.store.bulk_set(items, debug=debug)
        
        # Update indexes
        if self.fulltext_index:
            for key, value in items:
                self.fulltext_index.index_value(key, value)
        if self.embedding_index:
            for key, value in items:
                self.embedding_index.index_value(key, value)
        
        return result
    
    def search_fulltext(self, query: str, limit: int = 10) -> List[Tuple[str, float]]:
        """Search using full-text index"""
        if not self.fulltext_index:
            raise Exception("Full-text index not enabled")
        return self.fulltext_index.search(query, limit)
    
    def search_similar(self, query: str, limit: int = 10) -> List[Tuple[str, float]]:
        """Search using word embedding index"""
        if not self.embedding_index:
            raise Exception("Word embedding index not enabled")
        return self.embedding_index.search(query, limit)
    
    def checkpoint(self):
        """Create checkpoint"""
        self.store.checkpoint()
