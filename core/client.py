"""
Client for KV Store
"""
import requests
from typing import List, Tuple, Optional, Any


class KVStoreClient:
    """Client for interacting with KV Store server"""
    
    def __init__(self, host="127.0.0.1", port=5000):
        self.base_url = f"http://{host}:{port}"
        self.session = requests.Session()
    
    def set(self, key: str, value: Any, debug: Optional[bool] = None) -> bool:
        """Set a key-value pair"""
        response = self.session.post(
            f"{self.base_url}/set",
            json={'key': key, 'value': value, 'debug': debug}
        )
        response.raise_for_status()
        result = response.json()
        if result['status'] != 'ok':
            raise Exception(result.get('error', 'Unknown error'))
        return True
    
    def get(self, key: str) -> Optional[Any]:
        """Get value by key"""
        response = self.session.get(f"{self.base_url}/get/{key}")
        response.raise_for_status()
        result = response.json()
        if result['status'] != 'ok':
            raise Exception(result.get('error', 'Unknown error'))
        return result.get('value')
    
    def delete(self, key: str) -> bool:
        """Delete a key"""
        response = self.session.delete(f"{self.base_url}/delete/{key}")
        response.raise_for_status()
        result = response.json()
        if result['status'] != 'ok':
            raise Exception(result.get('error', 'Unknown error'))
        return True
    
    def bulk_set(self, items: List[Tuple[str, Any]], debug: Optional[bool] = None) -> bool:
        """Bulk set multiple key-value pairs"""
        # Convert tuples to list of lists for JSON serialization
        items_list = [[k, v] for k, v in items]
        response = self.session.post(
            f"{self.base_url}/bulk_set",
            json={'items': items_list, 'debug': debug}
        )
        response.raise_for_status()
        result = response.json()
        if result['status'] != 'ok':
            raise Exception(result.get('error', 'Unknown error'))
        return True
    
    def checkpoint(self) -> bool:
        """Request server to create checkpoint"""
        response = self.session.post(f"{self.base_url}/checkpoint")
        response.raise_for_status()
        result = response.json()
        if result['status'] != 'ok':
            raise Exception(result.get('error', 'Unknown error'))
        return True
    
    def health(self) -> bool:
        """Check if server is healthy"""
        try:
            response = self.session.get(f"{self.base_url}/health")
            return response.status_code == 200
        except:
            return False
    
    def close(self):
        """Close the client (cleanup)"""
        self.session.close()
