"""
Configuration module for DHT nodes
Supports Phase 2 (replication) and Phase 3 (datacenter awareness)
"""

from typing import List, Dict, Set


class NodeConfig:
    """
    Configuration for DHT nodes.
    Supports tunable consistency, replication, and datacenter awareness.
    """
    
    N = 3
    R = 2
    W = 2
    
    NUM_VIRTUAL_NODES = 150
    
    ENABLE_READ_REPAIR = True
    READ_REPAIR_ASYNC = False
    
    # Hinted Handoff Configuration
    ENABLE_HINTED_HANDOFF = True
    HINT_DELIVERY_INTERVAL_SEC = 30  # How often to check for hint delivery (seconds)
    HINT_MAX_AGE_SEC = 600  # 10 minutes - delete hints older than this
    HINT_MAX_ATTEMPTS = 100  # Maximum delivery attempts before giving up
    SLOPPY_QUORUM_EXTRA_NODES = 2  # How many extra nodes to try for sloppy quorum
    
    DATACENTER_MAP: Dict[str, str] = {
    }
    
    DATACENTER_AWARE = False
    
    REQUEST_TIMEOUT_MS = 5000
    REPLICATION_TIMEOUT_MS = 3000
    
    @staticmethod
    def get_datacenter(node_address: str) -> str:
        """
        Get the datacenter ID for a given node address.
        """
        return NodeConfig.DATACENTER_MAP.get(node_address, "UNKNOWN")
    
    @staticmethod
    def add_node_to_datacenter(node_address: str, datacenter_id: str):
        """
        Add a node to a specific datacenter.
        """
        NodeConfig.DATACENTER_MAP[node_address] = datacenter_id
        print(f"[Config] Added {node_address} to {datacenter_id}")
    
    @staticmethod
    def get_all_datacenters() -> Set[str]:
        """
        Get all unique datacenter IDs.
        """
        return set(NodeConfig.DATACENTER_MAP.values())
    
    @staticmethod
    def get_nodes_in_datacenter(datacenter_id: str) -> List[str]:
        """
        Get all nodes in a specific datacenter.
        """
        return [node for node, dc in NodeConfig.DATACENTER_MAP.items() 
                if dc == datacenter_id]
    
    @staticmethod
    def validate_quorum_config() -> bool:
        """
        Validate that quorum configuration makes sense.
        For strong consistency: R + W > N
        """
        if NodeConfig.R + NodeConfig.W > NodeConfig.N:
            print("[Config] Strong consistency: R + W > N ✓")
            return True
        else:
            print("[Config] Warning: Eventual consistency: R + W <= N")
            return False
    
    @staticmethod
    def set_strong_consistency():
        """Set parameters for strong consistency."""
        NodeConfig.R = 2
        NodeConfig.W = 2
        NodeConfig.N = 3
        print("[Config] Set to STRONG consistency (R=2, W=2, N=3)")
    
    @staticmethod
    def set_eventual_consistency():
        """Set parameters for eventual consistency."""
        NodeConfig.R = 1
        NodeConfig.W = 1
        NodeConfig.N = 3
        print("[Config] Set to EVENTUAL consistency (R=1, W=1, N=3)")
