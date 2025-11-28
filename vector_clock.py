"""
Vector Clock Implementation for Conflict Detection
Tracks causality between concurrent writes in the distributed system
"""

from typing import Dict, Optional, Tuple
import copy


class VectorClock:
    """
    Vector clock implementation for tracking causality and detecting conflicts.
    Each node maintains a counter in the vector clock.
    """
    
    def __init__(self, clock: Optional[Dict[str, int]] = None):
        """
        Initialize a vector clock.
        """
        self.clock: Dict[str, int] = clock or {}
    
    def increment(self, node_id: str) -> 'VectorClock':
        """
        Increment the counter for a specific node.
        """
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
        return self
    
    def update(self, other: 'VectorClock') -> 'VectorClock':
        """
        Update this vector clock with another (merge operation).
        Takes the maximum counter value for each node.
        """
        for node_id, counter in other.clock.items():
            self.clock[node_id] = max(self.clock.get(node_id, 0), counter)
        return self
    
    def compare(self, other: 'VectorClock') -> str:
        """
        Compare this vector clock with another to determine causality.
        """
        if not other or not other.clock:
            return "AFTER" if self.clock else "EQUAL"
        
        if not self.clock:
            return "BEFORE"
        
        all_nodes = set(self.clock.keys()) | set(other.clock.keys())
        
        self_greater = False
        other_greater = False
        
        for node in all_nodes:
            self_val = self.clock.get(node, 0)
            other_val = other.clock.get(node, 0)
            
            if self_val > other_val:
                self_greater = True
            elif self_val < other_val:
                other_greater = True
        
        if self_greater and other_greater:
            return "CONCURRENT"
        elif self_greater:
            return "AFTER"
        elif other_greater:
            return "BEFORE"
        else:
            return "EQUAL"
    
    def is_before(self, other: 'VectorClock') -> bool:
        """Check if this clock is causally before another."""
        return self.compare(other) == "BEFORE"
    
    def is_after(self, other: 'VectorClock') -> bool:
        """Check if this clock is causally after another."""
        return self.compare(other) == "AFTER"
    
    def is_concurrent(self, other: 'VectorClock') -> bool:
        """Check if this clock is concurrent with another (conflict)."""
        return self.compare(other) == "CONCURRENT"
    
    def copy(self) -> 'VectorClock':
        """Create a deep copy of this vector clock."""
        return VectorClock(copy.deepcopy(self.clock))
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary format (for gRPC transmission)."""
        return dict(self.clock)
    
    @staticmethod
    def from_dict(clock_dict: Dict[str, int]) -> 'VectorClock':
        """Create a vector clock from a dictionary."""
        return VectorClock(dict(clock_dict))
    
    def __str__(self) -> str:
        """String representation of the vector clock."""
        items = [f"{node}:{count}" for node, count in sorted(self.clock.items())]
        return "{" + ", ".join(items) + "}"
    
    def __repr__(self) -> str:
        return f"VectorClock({self.clock})"
    
    def __eq__(self, other) -> bool:
        """Check equality with another vector clock."""
        if not isinstance(other, VectorClock):
            return False
        return self.compare(other) == "EQUAL"


class VersionedValue:
    """
    A versioned value with vector clock for conflict detection.
    Stores value along with its version history.
    """
    
    def __init__(self, value: str, vector_clock: VectorClock):
        """
        Initialize a versioned value.
        
        Args:
            value: The actual value
            vector_clock: Vector clock representing this version
        """
        self.value = value
        self.vector_clock = vector_clock
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for storage."""
        return {
            'value': self.value,
            'vector_clock': self.vector_clock.to_dict()
        }
    
    @staticmethod
    def from_dict(data: Dict) -> 'VersionedValue':
        """Create from dictionary."""
        return VersionedValue(
            value=data['value'],
            vector_clock=VectorClock.from_dict(data.get('vector_clock', {}))
        )
    
    def __str__(self) -> str:
        return f"VersionedValue(value='{self.value}', clock={self.vector_clock})"


def reconcile_versions(versions: list) -> Tuple[list, list]:
    """
    Reconcile multiple versions to find the latest and detect conflicts.
    """
    if not versions:
        return [], []
    
    if len(versions) == 1:
        return versions, []
    
    latest = []
    
    for i, v1 in enumerate(versions):
        is_dominated = False
        for j, v2 in enumerate(versions):
            if i != j and v1.vector_clock.is_before(v2.vector_clock):
                is_dominated = True
                break
        
        if not is_dominated:
            latest.append(v1)
    
    if len(latest) > 1:
        first_version = latest[0]
        all_same = True
        for v in latest[1:]:
            if (v.value != first_version.value or 
                v.vector_clock.compare(first_version.vector_clock) != "EQUAL"):
                all_same = False
                break
        
        conflicts = [] if all_same else latest
    else:
        conflicts = []
    
    return latest, conflicts
