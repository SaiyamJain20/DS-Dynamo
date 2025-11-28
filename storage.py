"""
Node Storage Module
Handles in-memory key-value storage for the DHT node
Supports versioning with vector clocks for conflict detection
"""

from typing import Dict, Optional, Any, List
from threading import Lock
import json
from vector_clock import VectorClock, VersionedValue, reconcile_versions


class NodeStorage:
    """
    Thread-safe in-memory storage for key-value pairs.
    Supports versioning with vector clocks for conflict detection (Phase 2+).
    Stores multiple versions (siblings) for each key when conflicts occur.
    """
    
    def __init__(self, node_id: str):
        """
        Initialize the storage.
        
        Args:
            node_id: Unique identifier for this node
        """
        self.node_id = node_id
        self.data: Dict[str, List[VersionedValue]] = {}
        self.lock = Lock()
        
    def put(self, key: str, value: str, vector_clock: Optional[VectorClock] = None, 
            is_replication: bool = False) -> VectorClock:
        """
        Store a key-value pair with versioning.
        Uses vector clocks to detect and handle conflicts.
        """
        with self.lock:
            try:
                if vector_clock is None:
                    vector_clock = VectorClock()
                
                if not is_replication:
                    vector_clock = vector_clock.copy()
                    vector_clock.increment(self.node_id)
                
                new_version = VersionedValue(value, vector_clock)
                
                if key not in self.data:
                    self.data[key] = [new_version]
                    print(f"[{self.node_id}] Stored NEW key='{key}', value='{value}', clock={vector_clock}")
                else:
                    existing_versions = self.data[key]
                    
                    updated_versions = []
                    has_conflict = False
                    
                    for existing in existing_versions:
                        comparison = vector_clock.compare(existing.vector_clock)
                        
                        if comparison == "AFTER":
                            continue
                        elif comparison == "BEFORE":
                            print(f"[{self.node_id}] Version conflict: old dominates for key='{key}'")
                            updated_versions.append(existing)
                        elif comparison == "CONCURRENT":
                            has_conflict = True
                            updated_versions.append(existing)
                    
                    updated_versions.append(new_version)
                    self.data[key] = updated_versions
                    
                    if has_conflict:
                        print(f"[{self.node_id}] CONFLICT detected for key='{key}', siblings={len(updated_versions)}")
                    else:
                        print(f"[{self.node_id}] Updated key='{key}', value='{value}', clock={vector_clock}")
                
                return vector_clock
                
            except Exception as e:
                print(f"[{self.node_id}] Error storing key='{key}': {e}")
                return vector_clock or VectorClock()
    
    def get(self, key: str) -> Optional[List[VersionedValue]]:
        """
        Retrieve all versions for a key.
        Returns list of versioned values (siblings if conflicts exist).
        """
        with self.lock:
            if key in self.data:
                versions = self.data[key]
                print(f"[{self.node_id}] Retrieved key='{key}', versions={len(versions)}")
                return versions
            else:
                print(f"[{self.node_id}] Key='{key}' not found")
                return None
    
    def get_latest(self, key: str) -> Optional[VersionedValue]:
        """
        Get the latest non-conflicting version of a key.
        If conflicts exist, returns the first sibling.
        """
        versions = self.get(key)
        if not versions:
            return None
        
        latest_versions, conflicts = reconcile_versions(versions)
        
        if conflicts:
            print(f"[{self.node_id}] Warning: Multiple versions exist for key='{key}'")
        
        return latest_versions[0] if latest_versions else None
    
    def has_key(self, key: str) -> bool:
        """
        Check if a key exists in storage.
        """
        with self.lock:
            return key in self.data
    
    def merge_versions(self, key: str, versions: List[VersionedValue]) -> None:
        """
        Merge multiple versions for a key (used in read-repair).
        """
        with self.lock:
            if key not in self.data:
                self.data[key] = versions
                print(f"[{self.node_id}] Merged {len(versions)} versions for new key='{key}'")
                return
            
            all_versions = self.data[key] + versions
            
            unique_versions = []
            seen_clocks = set()
            
            for v in all_versions:
                clock_str = str(v.vector_clock)
                if clock_str not in seen_clocks:
                    seen_clocks.add(clock_str)
                    unique_versions.append(v)
            
            latest_versions, _ = reconcile_versions(unique_versions)
            self.data[key] = latest_versions
            
            print(f"[{self.node_id}] Merged versions for key='{key}', result={len(latest_versions)} versions")
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from storage.
        """
        with self.lock:
            if key in self.data:
                del self.data[key]
                print(f"[{self.node_id}] Deleted key='{key}'")
                return True
            return False
    
    def get_all_keys(self) -> list:
        """
        Get all keys stored in this node.
        """
        with self.lock:
            return list(self.data.keys())
    
    def size(self) -> int:
        """
        Get the number of keys stored.
        """
        with self.lock:
            return len(self.data)
    
    def clear(self) -> None:
        """
        Clear all data from storage.
        """
        with self.lock:
            self.data.clear()
            print(f"[{self.node_id}] Storage cleared")
    
    def __str__(self) -> str:
        """String representation of storage state."""
        with self.lock:
            return f"NodeStorage(node_id={self.node_id}, keys={len(self.data)})"
