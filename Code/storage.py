"""
Node Storage Module
Handles in-memory key-value storage for the DHT node
Supports versioning with vector clocks for conflict detection
Includes persistent storage to disk for crash recovery
"""

from typing import Dict, Optional, Any, List
from threading import Lock
import json
import os
import time
from vector_clock import VectorClock, VersionedValue, reconcile_versions


class HintedValue:
    """
    Represents a value stored as a hint for another node.
    Contains metadata about the hint: target node, timestamp, delivery attempts.
    """
    def __init__(self, value: str, vector_clock: VectorClock, target_node: str):
        self.value = value
        self.vector_clock = vector_clock
        self.target_node = target_node
        self.timestamp = time.time()
        self.attempts = 0
        self.last_attempt = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization"""
        return {
            'value': self.value,
            'vector_clock': self.vector_clock.to_dict(),
            'target_node': self.target_node,
            'timestamp': self.timestamp,
            'attempts': self.attempts,
            'last_attempt': self.last_attempt
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'HintedValue':
        """Create from dictionary"""
        hv = HintedValue(
            value=data['value'],
            vector_clock=VectorClock.from_dict(data['vector_clock']),
            target_node=data['target_node']
        )
        hv.timestamp = data.get('timestamp', time.time())
        hv.attempts = data.get('attempts', 0)
        hv.last_attempt = data.get('last_attempt')
        return hv


class NodeStorage:
    """
    Thread-safe in-memory storage for key-value pairs.
    Supports versioning with vector clocks for conflict detection (Phase 2+).
    Stores multiple versions (siblings) for each key when conflicts occur.
    """
    
    def __init__(self, node_id: str, data_dir: str = "data"):
        """
        Initialize the storage.
        
        Args:
            node_id: Unique identifier for this node
            data_dir: Directory to store persistent data
        """
        self.node_id = node_id
        self.data: Dict[str, List[VersionedValue]] = {}
        self.hints: Dict[str, Dict[str, HintedValue]] = {}  # {target_node: {key: HintedValue}}
        self.lock = Lock()
        self.hints_lock = Lock()
        self.data_dir = data_dir
        self.data_file = os.path.join(data_dir, f"{node_id}_data.json")
        self.hints_file = os.path.join(data_dir, f"{node_id}_hints.json")
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Load existing data from disk if available
        self.load_from_disk()
        self.load_hints_from_disk()
        
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
                
                # Save to disk after every put operation
                self.save_to_disk()
                
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
            
            # Save to disk after merge
            self.save_to_disk()
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from storage.
        """
        with self.lock:
            if key in self.data:
                del self.data[key]
                print(f"[{self.node_id}] Deleted key='{key}'")
                
                # Save to disk after delete
                self.save_to_disk()
                
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
    
    def save_to_disk(self) -> bool:
        """
        Save all data to disk in JSON format.
        Serializes key-value pairs along with vector clocks.
        
        Note: This method should be called while holding self.lock
        
        Returns:
            bool: True if save was successful, False otherwise
        """
        try:
            # Convert data to serializable format (no lock needed - caller must hold it)
            serializable_data = {}
            for key, versions in self.data.items():
                serializable_data[key] = [
                    {
                        'value': vv.value,
                        'vector_clock': vv.vector_clock.to_dict()
                    }
                    for vv in versions
                ]
            
            # Write to temporary file first for atomicity
            temp_file = self.data_file + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(serializable_data, f, indent=2)
            
            # Rename to actual file (atomic on POSIX)
            os.replace(temp_file, self.data_file)
            
            print(f"[{self.node_id}] Saved {len(self.data)} keys to disk")
            return True
                
        except Exception as e:
            print(f"[{self.node_id}] Error saving to disk: {e}")
            return False
    
    def load_from_disk(self) -> bool:
        """
        Load data from disk if the data file exists.
        Deserializes key-value pairs along with vector clocks.
        
        Returns:
            bool: True if load was successful, False if file doesn't exist or error occurred
        """
        if not os.path.exists(self.data_file):
            print(f"[{self.node_id}] No existing data file found, starting fresh")
            return False
        
        try:
            with open(self.data_file, 'r') as f:
                serializable_data = json.load(f)
            
            # Convert back to VersionedValue objects
            loaded_data = {}
            for key, versions_data in serializable_data.items():
                versions = []
                for version_data in versions_data:
                    vc = VectorClock.from_dict(version_data['vector_clock'])
                    vv = VersionedValue(version_data['value'], vc)
                    versions.append(vv)
                loaded_data[key] = versions
            
            with self.lock:
                self.data = loaded_data
            
            print(f"[{self.node_id}] Loaded {len(self.data)} keys from disk")
            return True
            
        except Exception as e:
            print(f"[{self.node_id}] Error loading from disk: {e}")
            return False
    
    # ==================== HINTED HANDOFF METHODS ====================
    
    def put_with_hint(self, key: str, value: str, vector_clock: VectorClock, 
                      target_node: str) -> bool:
        """
        Store a key-value pair as a hint for another node.
        This is used during sloppy quorum when a node in the preference list is down.
        
        Args:
            key: The key to store
            value: The value to store
            vector_clock: Vector clock for this version
            target_node: Address of the node this data belongs to
        
        Returns:
            bool: True if hint was stored successfully
        """
        with self.hints_lock:
            try:
                hinted_value = HintedValue(value, vector_clock, target_node)
                
                if target_node not in self.hints:
                    self.hints[target_node] = {}
                
                # If key already exists as hint for this target, update it
                if key in self.hints[target_node]:
                    old_hint = self.hints[target_node][key]
                    comparison = vector_clock.compare(old_hint.vector_clock)
                    
                    if comparison == "AFTER":
                        # New version supersedes old
                        self.hints[target_node][key] = hinted_value
                        print(f"[{self.node_id}] Updated HINT for {target_node}: key='{key}'")
                    elif comparison == "CONCURRENT":
                        # Keep both versions - store as list? For now, keep newer timestamp
                        print(f"[{self.node_id}] Hint conflict for {target_node}: key='{key}', keeping newer")
                        self.hints[target_node][key] = hinted_value
                    else:
                        print(f"[{self.node_id}] Ignoring older hint for {target_node}: key='{key}'")
                        return True
                else:
                    self.hints[target_node][key] = hinted_value
                    print(f"[{self.node_id}] Stored NEW HINT for {target_node}: key='{key}'")
                
                # Save hints to disk
                self.save_hints_to_disk()
                return True
                
            except Exception as e:
                print(f"[{self.node_id}] Error storing hint for {target_node}, key='{key}': {e}")
                return False
    
    def get_all_hints(self) -> Dict[str, Dict[str, HintedValue]]:
        """
        Get all hints stored on this node.
        
        Returns:
            Dict mapping target_node -> {key: HintedValue}
        """
        with self.hints_lock:
            return dict(self.hints)
    
    def get_hints_for_node(self, target_node: str) -> Dict[str, HintedValue]:
        """
        Get all hints destined for a specific node.
        
        Args:
            target_node: Address of the target node
        
        Returns:
            Dict mapping key -> HintedValue for that target node
        """
        with self.hints_lock:
            return dict(self.hints.get(target_node, {}))
    
    def remove_hint(self, target_node: str, key: str) -> bool:
        """
        Remove a specific hint after successful delivery.
        
        Args:
            target_node: Address of the target node
            key: The key to remove
        
        Returns:
            bool: True if hint was found and removed
        """
        with self.hints_lock:
            if target_node in self.hints and key in self.hints[target_node]:
                del self.hints[target_node][key]
                
                # Clean up empty target node entries
                if not self.hints[target_node]:
                    del self.hints[target_node]
                
                print(f"[{self.node_id}] Removed HINT for {target_node}: key='{key}'")
                self.save_hints_to_disk()
                return True
            return False
    
    def remove_all_hints_for_node(self, target_node: str) -> int:
        """
        Remove all hints for a specific target node.
        Useful when a node is permanently removed from the cluster.
        
        Args:
            target_node: Address of the target node
        
        Returns:
            int: Number of hints removed
        """
        with self.hints_lock:
            if target_node in self.hints:
                count = len(self.hints[target_node])
                del self.hints[target_node]
                print(f"[{self.node_id}] Removed ALL {count} hints for {target_node}")
                self.save_hints_to_disk()
                return count
            return 0
    
    def increment_hint_attempts(self, target_node: str, key: str) -> None:
        """
        Increment the delivery attempt counter for a hint.
        
        Args:
            target_node: Address of the target node
            key: The key
        """
        with self.hints_lock:
            if target_node in self.hints and key in self.hints[target_node]:
                hint = self.hints[target_node][key]
                hint.attempts += 1
                hint.last_attempt = time.time()
                self.save_hints_to_disk()
    
    def get_hint_count(self) -> int:
        """
        Get total number of hints stored.
        
        Returns:
            int: Total number of hints across all target nodes
        """
        with self.hints_lock:
            return sum(len(hints) for hints in self.hints.values())
    
    def get_hint_stats(self) -> dict:
        """
        Get statistics about stored hints.
        
        Returns:
            dict: Statistics including total hints, hints per target, oldest hint age
        """
        with self.hints_lock:
            total_hints = 0
            hints_per_target = {}
            oldest_timestamp = time.time()
            
            for target_node, hints in self.hints.items():
                count = len(hints)
                total_hints += count
                hints_per_target[target_node] = count
                
                for hint in hints.values():
                    if hint.timestamp < oldest_timestamp:
                        oldest_timestamp = hint.timestamp
            
            oldest_age = time.time() - oldest_timestamp if total_hints > 0 else 0
            
            return {
                'total_hints': total_hints,
                'target_nodes': len(self.hints),
                'hints_per_target': hints_per_target,
                'oldest_hint_age_seconds': oldest_age
            }
    
    def save_hints_to_disk(self) -> bool:
        """
        Save all hints to disk in JSON format.
        Should be called while holding self.hints_lock.
        
        Returns:
            bool: True if save was successful
        """
        try:
            serializable_hints = {}
            for target_node, hints in self.hints.items():
                serializable_hints[target_node] = {
                    key: hint.to_dict()
                    for key, hint in hints.items()
                }
            
            temp_file = self.hints_file + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(serializable_hints, f, indent=2)
            
            os.replace(temp_file, self.hints_file)
            
            hint_count = sum(len(hints) for hints in self.hints.values())
            print(f"[{self.node_id}] Saved {hint_count} hints to disk")
            return True
            
        except Exception as e:
            print(f"[{self.node_id}] Error saving hints to disk: {e}")
            return False
    
    def load_hints_from_disk(self) -> bool:
        """
        Load hints from disk if the hints file exists.
        
        Returns:
            bool: True if load was successful
        """
        if not os.path.exists(self.hints_file):
            print(f"[{self.node_id}] No existing hints file found, starting fresh")
            return False
        
        try:
            with open(self.hints_file, 'r') as f:
                serializable_hints = json.load(f)
            
            loaded_hints = {}
            for target_node, hints_data in serializable_hints.items():
                loaded_hints[target_node] = {}
                for key, hint_data in hints_data.items():
                    hint = HintedValue.from_dict(hint_data)
                    loaded_hints[target_node][key] = hint
            
            with self.hints_lock:
                self.hints = loaded_hints
            
            hint_count = sum(len(hints) for hints in self.hints.values())
            print(f"[{self.node_id}] Loaded {hint_count} hints from disk")
            return True
            
        except Exception as e:
            print(f"[{self.node_id}] Error loading hints from disk: {e}")
            return False


