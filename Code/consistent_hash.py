"""
Consistent Hashing Implementation with Virtual Nodes
Supports data partitioning across distributed nodes
Includes persistent storage for node positions
"""

import hashlib
import json
import os
from typing import List, Optional, Dict
from bisect import bisect_right


class ConsistentHash:
    """
    Consistent Hashing Ring implementation with virtual nodes support.
    Maps both nodes and keys to positions on a circular hash ring.
    """
    
    def __init__(self, num_virtual_nodes: int = 15):
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
        When datacenter_aware=True, prefers cross-datacenter replication
        while maintaining consistent hashing ring order.
        """
        if not self.ring:
            return []
        
        preference_list = []
        hash_value = self._hash(key)
        
        idx = bisect_right(self.sorted_keys, hash_value)
        
        if not datacenter_aware or not datacenter_map:
            # Simple mode: just get N unique nodes clockwise from key position
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
            # Datacenter-aware mode: walk the ring and prefer nodes from different DCs
            visited_nodes = set()
            visited_dcs = set()
            
            # Collect candidates in ring order, separating by DC status
            candidates_new_dc = []  # Nodes from unvisited DCs (in ring order)
            candidates_same_dc = []  # Nodes from visited DCs (in ring order)
            
            attempts = 0
            max_attempts = len(self.sorted_keys) * 2
            current_idx = idx
            
            while len(preference_list) < n and attempts < max_attempts:
                if current_idx >= len(self.sorted_keys):
                    current_idx = 0
                
                node_id = self.ring[self.sorted_keys[current_idx]]
                
                if node_id not in visited_nodes:
                    node_dc = datacenter_map.get(node_id, "UNKNOWN")
                    
                    if node_dc not in visited_dcs:
                        # Immediately add nodes from new datacenters
                        preference_list.append(node_id)
                        visited_nodes.add(node_id)
                        visited_dcs.add(node_dc)
                    else:
                        # Save nodes from already-visited DCs for later
                        candidates_same_dc.append(node_id)
                        visited_nodes.add(node_id)
                
                current_idx += 1
                attempts += 1
            
            # If we still need more nodes and have candidates from same DCs, add them
            for node_id in candidates_same_dc:
                if len(preference_list) >= n:
                    break
                preference_list.append(node_id)
        
        return preference_list
    
    def get_all_nodes(self) -> List[str]:
        """
        Get all physical nodes in the ring.
        """
        return list(self.nodes)
    
    def __str__(self) -> str:
        """String representation of the hash ring."""
        return f"ConsistentHash(nodes={len(self.nodes)}, virtual_nodes={len(self.ring)})"
    
    def save_ring_state(self, node_id: str, data_dir: str = "data") -> bool:
        """
        Save the current hash ring state to disk for a specific node.
        This includes all nodes in the ring and their virtual node positions.
        
        Args:
            node_id: The node ID for which to save the state
            data_dir: Directory to store the ring state
            
        Returns:
            bool: True if save was successful
        """
        try:
            os.makedirs(data_dir, exist_ok=True)
            ring_file = os.path.join(data_dir, f"{node_id}_ring.json")
            
            # Serialize ring state
            state = {
                'num_virtual_nodes': self.num_virtual_nodes,
                'nodes': list(self.nodes),
                'ring': {str(k): v for k, v in self.ring.items()},
                'sorted_keys': self.sorted_keys
            }
            
            # Write to temporary file first
            temp_file = ring_file + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            # Atomic rename
            os.replace(temp_file, ring_file)
            
            print(f"[ConsistentHash] Saved ring state for {node_id}")
            return True
            
        except Exception as e:
            print(f"[ConsistentHash] Error saving ring state: {e}")
            return False
    
    def load_ring_state(self, node_id: str, data_dir: str = "data") -> bool:
        """
        Load the hash ring state from disk for a specific node.
        Restores all nodes in the ring and their virtual node positions.
        
        Args:
            node_id: The node ID for which to load the state
            data_dir: Directory where the ring state is stored
            
        Returns:
            bool: True if load was successful
        """
        ring_file = os.path.join(data_dir, f"{node_id}_ring.json")
        
        if not os.path.exists(ring_file):
            print(f"[ConsistentHash] No ring state file found for {node_id}")
            return False
        
        try:
            with open(ring_file, 'r') as f:
                state = json.load(f)
            
            # Restore ring state
            self.num_virtual_nodes = state['num_virtual_nodes']
            self.nodes = set(state['nodes'])
            self.ring = {int(k): v for k, v in state['ring'].items()}
            self.sorted_keys = state['sorted_keys']
            
            print(f"[ConsistentHash] Loaded ring state for {node_id}: {len(self.nodes)} nodes")
            return True
            
        except Exception as e:
            print(f"[ConsistentHash] Error loading ring state: {e}")
            return False

