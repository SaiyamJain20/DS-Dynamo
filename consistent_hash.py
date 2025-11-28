"""
Consistent Hashing Implementation with Virtual Nodes
Supports data partitioning across distributed nodes
"""

import hashlib
from typing import List, Optional
from bisect import bisect_right


class ConsistentHash:
    """
    Consistent Hashing Ring implementation with virtual nodes support.
    Maps both nodes and keys to positions on a circular hash ring.
    """
    
    def __init__(self, num_virtual_nodes: int = 3):
        """
        Initialize the consistent hash ring.
        """
        self.num_virtual_nodes = num_virtual_nodes
        self.ring = {}
        self.sorted_keys = []
        self.nodes = set() 
        
    def _hash(self, key: str) -> int:
        """
        Generate hash value for a given key.
        Uses MD5 hash and converts to integer.
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node_id: str) -> None:
        """
        Add a physical node to the hash ring.
        Creates multiple virtual nodes for better load distribution.
        """
        if node_id in self.nodes:
            print(f"Node {node_id} already exists in the ring")
            return
            
        self.nodes.add(node_id)
        
        for i in range(self.num_virtual_nodes):
            virtual_key = f"{node_id}:vnode{i}"
            hash_value = self._hash(virtual_key)
            self.ring[hash_value] = node_id
            self.sorted_keys.append(hash_value)
        
        self.sorted_keys.sort()
        print(f"Added node {node_id} with {self.num_virtual_nodes} virtual nodes")
    
    def remove_node(self, node_id: str) -> None:
        """
        Remove a physical node from the hash ring.
        Removes all its virtual nodes.
        """
        if node_id not in self.nodes:
            print(f"Node {node_id} not found in the ring")
            return
            
        self.nodes.remove(node_id)
        
        for i in range(self.num_virtual_nodes):
            virtual_key = f"{node_id}:vnode{i}"
            hash_value = self._hash(virtual_key)
            if hash_value in self.ring:
                del self.ring[hash_value]
                self.sorted_keys.remove(hash_value)
        
        print(f"Removed node {node_id}")
    
    def get_node(self, key: str) -> Optional[str]:
        """
        Get the node responsible for a given key.
        Walks clockwise on the ring from the key's hash position.
        """
        if not self.ring:
            return None
        
        hash_value = self._hash(key)
        
        idx = bisect_right(self.sorted_keys, hash_value)
        
        if idx == len(self.sorted_keys):
            idx = 0
        
        return self.ring[self.sorted_keys[idx]]
    
    def get_preference_list(self, key: str, n: int = 3, datacenter_aware: bool = False, 
                           datacenter_map: dict = None) -> List[str]:
        """
        Get the preference list of N nodes responsible for a key.
        """
        if not self.ring:
            return []
        
        preference_list = []
        hash_value = self._hash(key)
        
        idx = bisect_right(self.sorted_keys, hash_value)
        
        if not datacenter_aware or not datacenter_map:
            visited = set()
            attempts = 0
            max_attempts = len(self.sorted_keys) * 2
            
            while len(preference_list) < n and attempts < max_attempts:
                if idx >= len(self.sorted_keys):
                    idx = 0
                
                node_id = self.ring[self.sorted_keys[idx]]
                
                if node_id not in visited:
                    preference_list.append(node_id)
                    visited.add(node_id)
                
                idx += 1
                attempts += 1
        else:
            visited_nodes = set()
            visited_dcs = set()
            attempts = 0
            max_attempts = len(self.sorted_keys) * 2
            
            while len(preference_list) < n and attempts < max_attempts:
                if idx >= len(self.sorted_keys):
                    idx = 0
                
                node_id = self.ring[self.sorted_keys[idx]]
                node_dc = datacenter_map.get(node_id, "UNKNOWN")
                
                if node_id in visited_nodes:
                    idx += 1
                    attempts += 1
                    continue
                
                if node_dc not in visited_dcs or len(visited_dcs) >= len(set(datacenter_map.values())):
                    preference_list.append(node_id)
                    visited_nodes.add(node_id)
                    visited_dcs.add(node_dc)
                
                idx += 1
                attempts += 1
        
        return preference_list
    
    def get_all_nodes(self) -> List[str]:
        """
        Get all physical nodes in the ring.
        """
        return list(self.nodes)
    
    def __str__(self) -> str:
        """String representation of the hash ring."""
        return f"ConsistentHash(nodes={len(self.nodes)}, virtual_nodes={len(self.ring)})"
