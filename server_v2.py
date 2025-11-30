"""
DHT Node Server Implementation - Phases 2 & 3
Complete implementation with replication, versioning, and datacenter awareness
"""

import grpc
from concurrent import futures
import argparse
import sys
import time
import random
from typing import Dict, List, Optional
import threading

import dht_pb2
import dht_pb2_grpc

from consistent_hash import ConsistentHash
from storage import NodeStorage
from config import NodeConfig
from vector_clock import VectorClock, VersionedValue, reconcile_versions


class DHTServicer(dht_pb2_grpc.DHTServiceServicer):
    """
    gRPC service implementation for DHT operations.
    Supports replication, versioning, and datacenter-aware placement.
    """
    
    def __init__(self, node_id: str, host: str, port: int, known_nodes: list = None, 
                 datacenter: str = "UNKNOWN"):
        """
        Initialize the DHT service.
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.address = f"{host}:{port}"
        self.datacenter = datacenter
        
        # Initialize storage with persistence
        self.storage = NodeStorage(node_id)
        
        # Initialize hash ring and try to load previous state
        self.hash_ring = ConsistentHash(num_virtual_nodes=NodeConfig.NUM_VIRTUAL_NODES)
        ring_loaded = self.hash_ring.load_ring_state(node_id)
        
        # If ring state was loaded, this node should already be in the ring at its previous position
        if not ring_loaded:
            # Fresh start - add this node to the ring
            self.hash_ring.add_node(self.address)
            NodeConfig.add_node_to_datacenter(self.address, datacenter)
        else:
            # Node recovered from disk - verify it's in the ring
            if self.address not in self.hash_ring.get_all_nodes():
                print(f"[{self.node_id}] Node not in loaded ring, adding...")
                self.hash_ring.add_node(self.address)
            NodeConfig.add_node_to_datacenter(self.address, datacenter)
            print(f"[{self.node_id}] Recovered previous ring state with {len(self.hash_ring.get_all_nodes())} nodes")
        
        self.known_nodes: List[str] = []
        self.node_stubs: Dict[str, dht_pb2_grpc.DHTServiceStub] = {}
        
        # Add known nodes to the ring
        if known_nodes:
            for node in known_nodes:
                if node != self.address:
                    if node not in self.hash_ring.get_all_nodes():
                        self.hash_ring.add_node(node)
                    self.known_nodes.append(node)
        
        # Save the ring state after initialization
        self.hash_ring.save_ring_state(self.node_id)
        
        # Start hint delivery background thread if enabled
        self.hint_delivery_running = False
        self.hint_delivery_thread = None
        if NodeConfig.ENABLE_HINTED_HANDOFF:
            self.hint_delivery_running = True
            self.hint_delivery_thread = threading.Thread(
                target=self._hint_delivery_loop,
                daemon=True
            )
            self.hint_delivery_thread.start()
            print(f"[{self.node_id}] Started hint delivery thread")
        
        print(f"[{self.node_id}] Initialized at {self.address} (DC: {datacenter})")
        print(f"[{self.node_id}] Known nodes: {self.hash_ring.get_all_nodes()}")
        print(f"[{self.node_id}] Replication: N={NodeConfig.N}, R={NodeConfig.R}, W={NodeConfig.W}")
        print(f"[{self.node_id}] Datacenter-aware: {NodeConfig.DATACENTER_AWARE}")
        print(f"[{self.node_id}] Hinted handoff: {NodeConfig.ENABLE_HINTED_HANDOFF}")
    
    def initialize_cluster_membership(self):
        """
        Initialize cluster membership after server starts.
        Must be called after server is running to enable RPC calls.
        """
        # Discover all nodes in the cluster from known nodes
        if self.known_nodes:
            # Run discovery in a separate thread to not block server startup
            discovery_thread = threading.Thread(
                target=self._delayed_discovery,
                daemon=True
            )
            discovery_thread.start()
    
    def _delayed_discovery(self):
        """Run node discovery after a short delay."""
        time.sleep(1)  # Give server time to start
        
        # First, announce ourselves to known nodes
        self._announce_to_cluster()
        
        # Then discover other nodes
        self._discover_nodes()
    
    def _announce_to_cluster(self):
        """Announce this node's presence to known nodes."""
        if not self.known_nodes:
            return
        
        print(f"[{self.node_id}] Announcing presence to cluster...")
        
        for known_node in self.known_nodes:
            try:
                stub = self._get_node_stub(known_node)
                if not stub:
                    continue
                
                request = dht_pb2.JoinClusterRequest(
                    node_address=self.address,
                    datacenter=self.datacenter
                )
                response = stub.JoinCluster(request, timeout=5)
                
                if response.success:
                    print(f"[{self.node_id}] Successfully announced to {known_node}: {response.message}")
                    break  # Successfully announced to one node
                    
            except Exception as e:
                print(f"[{self.node_id}] Error announcing to {known_node}: {e}")
                continue
    
    def _hint_delivery_loop(self):
        """
        Background thread that periodically checks for hints and delivers them
        to target nodes when they become available.
        """
        print(f"[{self.node_id}] Hint delivery loop started")
        
        while self.hint_delivery_running:
            try:
                # Sleep first before checking
                time.sleep(NodeConfig.HINT_DELIVERY_INTERVAL_SEC)
                
                # Get all hints
                all_hints = self.storage.get_all_hints()
                
                if not all_hints:
                    continue
                
                hint_count = sum(len(hints) for hints in all_hints.values())
                print(f"[{self.node_id}] Hint delivery check: {hint_count} hints for {len(all_hints)} targets")
                
                # Process each target node
                for target_node, hints in list(all_hints.items()):
                    if not self.hint_delivery_running:
                        break
                    
                    # Check if target node is alive
                    if not self._is_node_alive(target_node):
                        print(f"[{self.node_id}] Target {target_node} still down, keeping hints")
                        continue
                    
                    # Target is alive - deliver hints
                    print(f"[{self.node_id}] Target {target_node} is ALIVE! Delivering {len(hints)} hints...")
                    
                    delivered_keys = []
                    
                    for key, hinted_value in hints.items():
                        # Check if hint is too old
                        age = time.time() - hinted_value.timestamp
                        if age > NodeConfig.HINT_MAX_AGE_SEC:
                            print(f"[{self.node_id}] Hint for {target_node} key='{key}' too old ({age:.0f}s), discarding")
                            delivered_keys.append(key)
                            continue
                        
                        # Check if too many attempts
                        if hinted_value.attempts >= NodeConfig.HINT_MAX_ATTEMPTS:
                            print(f"[{self.node_id}] Hint for {target_node} key='{key}' max attempts reached, discarding")
                            delivered_keys.append(key)
                            continue
                        
                        # Attempt delivery
                        self.storage.increment_hint_attempts(target_node, key)
                        
                        try:
                            success = self._deliver_hint_to_node(
                                target_node, key, 
                                hinted_value.value, 
                                hinted_value.vector_clock
                            )
                            
                            if success:
                                print(f"[{self.node_id}] Successfully delivered hint to {target_node}: key='{key}'")
                                delivered_keys.append(key)
                            else:
                                print(f"[{self.node_id}] Failed to deliver hint to {target_node}: key='{key}'")
                                
                        except Exception as e:
                            print(f"[{self.node_id}] Error delivering hint to {target_node}, key='{key}': {e}")
                    
                    # Remove successfully delivered hints
                    for key in delivered_keys:
                        self.storage.remove_hint(target_node, key)
                    
                    if delivered_keys:
                        print(f"[{self.node_id}] Delivered {len(delivered_keys)}/{len(hints)} hints to {target_node}")
                
            except Exception as e:
                print(f"[{self.node_id}] Error in hint delivery loop: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"[{self.node_id}] Hint delivery loop stopped")
    
    def _is_node_alive(self, node_address: str) -> bool:
        """
        Check if a node is alive and responsive.
        
        Args:
            node_address: Address of the node to check
        
        Returns:
            bool: True if node is responsive
        """
        try:
            stub = self._get_node_stub(node_address)
            if not stub:
                return False
            
            request = dht_pb2.HealthCheckRequest(node_id=self.node_id)
            response = stub.HealthCheck(request, timeout=2)
            
            return response.healthy
            
        except Exception as e:
            # Node is not responsive
            return False
    
    def _deliver_hint_to_node(self, target_node: str, key: str, 
                              value: str, vector_clock: VectorClock) -> bool:
        """
        Deliver a hint to the target node.
        
        Args:
            target_node: Address of the target node
            key: The key to deliver
            value: The value to deliver
            vector_clock: Vector clock for this version
        
        Returns:
            bool: True if delivery was successful
        """
        try:
            stub = self._get_node_stub(target_node)
            if not stub:
                return False
            
            request = dht_pb2.DeliverHintRequest(
                key=key,
                value=value,
                vector_clock=vector_clock.to_dict()
            )
            
            response = stub.DeliverHint(request, timeout=NodeConfig.REPLICATION_TIMEOUT_MS/1000)
            
            return response.success
            
        except Exception as e:
            print(f"[{self.node_id}] Error delivering hint to {target_node}: {e}")
            return False
    
    def _get_node_stub(self, node_address: str) -> Optional[dht_pb2_grpc.DHTServiceStub]:
        """
        Get or create a gRPC stub for communicating with another node.
        """
        if node_address == self.address:
            return None
        
        if node_address not in self.node_stubs:
            try:
                channel = grpc.insecure_channel(node_address)
                self.node_stubs[node_address] = dht_pb2_grpc.DHTServiceStub(channel)
            except Exception as e:
                print(f"[{self.node_id}] Failed to create stub for {node_address}: {e}")
                return None
        
        return self.node_stubs.get(node_address)
    
    def _replicate_to_node(self, key: str, value: str, vector_clock: VectorClock, 
                          node_address: str, results: dict, index: int):
        """
        Helper method to replicate to a single node (runs in thread).
        """
        try:
            stub = self._get_node_stub(node_address)
            if not stub:
                results[index] = False
                return
            
            request = dht_pb2.InternalPutRequest(
                key=key,
                value=value,
                vector_clock=vector_clock.to_dict(),
                is_replication=True
            )
            
            response = stub.InternalPut(request, timeout=NodeConfig.REPLICATION_TIMEOUT_MS/1000)
            
            if response.success:
                results[index] = True
                print(f"[{self.node_id}] Replicated to {node_address}")
            else:
                results[index] = False
                print(f"[{self.node_id}] Replication to {node_address} failed: {response.message}")
                
        except Exception as e:
            results[index] = False
            print(f"[{self.node_id}] Error replicating to {node_address}: {e}")
    
    def _replicate_put_with_hint(self, key: str, value: str, vector_clock: VectorClock,
                                 fallback_node: str, hint_for_node: str) -> bool:
        """
        Replicate data to a fallback node with a hint.
        Used for sloppy quorum when preference list nodes are unavailable.
        
        Args:
            fallback_node: The node to store the data temporarily
            hint_for_node: The node this data actually belongs to
        
        Returns:
            bool: True if write was successful
        """
        try:
            stub = self._get_node_stub(fallback_node)
            if not stub:
                return False
            
            request = dht_pb2.InternalPutWithHintRequest(
                key=key,
                value=value,
                vector_clock=vector_clock.to_dict(),
                hint_for_node=hint_for_node
            )
            
            response = stub.InternalPutWithHint(
                request, 
                timeout=NodeConfig.REPLICATION_TIMEOUT_MS/1000
            )
            
            if response.success:
                print(f"[{self.node_id}] Stored hint on {fallback_node} for {hint_for_node}")
                return True
            else:
                print(f"[{self.node_id}] Failed to store hint on {fallback_node}: {response.message}")
                return False
                
        except Exception as e:
            print(f"[{self.node_id}] Error storing hint on {fallback_node}: {e}")
            return False
    
    def _replicate_put(self, key: str, value: str, vector_clock: VectorClock, 
                       replica_nodes: List[str], coordinator_stored: bool = True) -> int:
        """
        Replicate a PUT operation to replica nodes using parallel replication.
        Returns as soon as W-1 additional writes succeed (W-quorum optimization).
        Remaining replications continue in background.
        
        Args:
            coordinator_stored: Whether coordinator already stored the key locally
        """
        # Count coordinator's write only if it actually stored the key
        successful_writes = 1 if coordinator_stored else 0
        required_additional_writes = NodeConfig.W - successful_writes  # Need this many more for quorum
        
        # CRITICAL FIX: Only filter out self if coordinator already stored locally
        # If coordinator is NOT in preference list, must forward to ALL N nodes
        if coordinator_stored:
            target_nodes = [node for node in replica_nodes if node != self.address]
        else:
            target_nodes = replica_nodes  # Forward to ALL nodes in preference list
        
        # If no target nodes, cannot proceed
        if not target_nodes:
            return successful_writes
        
        # If already have W writes, start background replication for remaining
        if required_additional_writes <= 0:
            # Start background replication for remaining nodes
            threading.Thread(
                target=self._background_replicate,
                args=(key, value, vector_clock, target_nodes),
                daemon=True
            ).start()
            return successful_writes
        
        # Start parallel replication threads
        threads = []
        results = {}
        
        for i, node_address in enumerate(target_nodes):
            thread = threading.Thread(
                target=self._replicate_to_node,
                args=(key, value, vector_clock, node_address, results, i)
            )
            thread.start()
            threads.append(thread)
        
        # Wait for W-1 successful writes (early return optimization)
        import time
        max_wait_time = NodeConfig.REPLICATION_TIMEOUT_MS / 1000
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            successful_additional = sum(1 for success in results.values() if success)
            
            if successful_additional >= required_additional_writes:
                # W-quorum achieved! Return immediately
                # Remaining threads continue in background
                print(f"[{self.node_id}] W-quorum achieved ({NodeConfig.W} writes), returning to client")
                print(f"[{self.node_id}] Background replication continues for remaining nodes")
                return successful_writes + successful_additional
            
            time.sleep(0.01)  # Small sleep to avoid busy waiting
        
        # Timeout reached, wait for all threads to finish and return final count
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=0.1)
        
        successful_writes += sum(1 for success in results.values() if success)
        return successful_writes
    
    def _background_replicate(self, key: str, value: str, vector_clock: VectorClock,
                             target_nodes: List[str]):
        """
        Background replication for remaining nodes after W-quorum is achieved.
        """
        results = {}
        threads = []
        
        for i, node_address in enumerate(target_nodes):
            thread = threading.Thread(
                target=self._replicate_to_node,
                args=(key, value, vector_clock, node_address, results, i)
            )
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        successful = sum(1 for success in results.values() if success)
        print(f"[{self.node_id}] Background replication completed: {successful}/{len(target_nodes)} nodes")
    
    def _replicate_get(self, key: str, replica_nodes: List[str]) -> List[VersionedValue]:
        """
        Read from multiple replicas and collect all versions.
        Coordinator reads locally first (if in preference list),
        then queries other nodes in the preference list.
        """
        all_versions = []
        successful_reads = 0
        
        # Check if coordinator is in the preference list
        coordinator_in_list = self.address in replica_nodes
        
        # If coordinator is in preference list, try local read first
        if coordinator_in_list:
            local_versions = self.storage.get(key)
            if local_versions:
                all_versions.extend(local_versions)
                successful_reads += 1
                print(f"[{self.node_id}] Read locally (coordinator in preference list)")
        
        # Query other nodes in preference list
        for node_address in replica_nodes:
            if node_address == self.address:
                continue  # Skip self, already read locally if applicable
            
            try:
                stub = self._get_node_stub(node_address)
                if not stub:
                    continue
                
                request = dht_pb2.InternalGetRequest(key=key)
                response = stub.InternalGet(request, timeout=NodeConfig.REQUEST_TIMEOUT_MS/1000)
                
                if response.success:
                    successful_reads += 1
                    for version_data in response.versions:
                        vc = VectorClock.from_dict(dict(version_data.vector_clock))
                        vv = VersionedValue(version_data.value, vc)
                        all_versions.append(vv)
                        
            except Exception as e:
                print(f"[{self.node_id}] Error reading from {node_address}: {e}")
        
        return all_versions
    
    def _read_repair(self, key: str, latest_versions: List[VersionedValue], 
                     replica_nodes: List[str]) -> None:
        """
        Perform read repair to synchronize replicas.
        """
        if not NodeConfig.ENABLE_READ_REPAIR:
            return
        
        print(f"[{self.node_id}] Performing read repair for key='{key}'")
        
        for node_address in replica_nodes:
            if node_address == self.address:
                continue
            
            try:
                stub = self._get_node_stub(node_address)
                if not stub:
                    continue
                
                versions_data = []
                for vv in latest_versions:
                    versions_data.append(dht_pb2.VersionedData(
                        value=vv.value,
                        vector_clock=vv.vector_clock.to_dict()
                    ))
                
                request = dht_pb2.ReadRepairRequest(
                    key=key,
                    versions=versions_data
                )
                
                response = stub.ReadRepair(request, timeout=NodeConfig.REPLICATION_TIMEOUT_MS/1000)
                
                if response.success:
                    print(f"[{self.node_id}] Read repair to {node_address} succeeded")
                    
            except Exception as e:
                print(f"[{self.node_id}] Read repair to {node_address} failed: {e}")
    
    def Put(self, request, context):
        """
        Handle client PUT request with replication and versioning.
        Acts as coordinator for this request.
        """
        try:
            key = request.key
            value = request.value
            
            if request.vector_clock:
                vector_clock = VectorClock.from_dict(dict(request.vector_clock))
            else:
                vector_clock = VectorClock()
            
            print(f"[{self.node_id}] Client PUT: key='{key}', value='{value}'")
            
            preference_list = self.hash_ring.get_preference_list(
                key, 
                n=NodeConfig.N,
                datacenter_aware=NodeConfig.DATACENTER_AWARE,
                datacenter_map=NodeConfig.DATACENTER_MAP
            )
            
            print(f"[{self.node_id}] Preference list for '{key}': {preference_list}")
            
            # Check if coordinator is in preference list
            coordinator_in_list = self.address in preference_list
            
            # Only store locally if coordinator is in the preference list
            if coordinator_in_list:
                final_clock = self.storage.put(key, value, vector_clock)
                print(f"[{self.node_id}] Coordinator IS in preference list, stored locally")
            else:
                # Coordinator not in preference list, just prepare the vector clock
                if not vector_clock or not vector_clock.clock:
                    vector_clock = VectorClock()
                vector_clock = vector_clock.copy()
                vector_clock.increment(self.node_id)
                final_clock = vector_clock
                print(f"[{self.node_id}] Coordinator NOT in preference list, forwarding only")
            
            successful_writes = self._replicate_put(key, value, final_clock, preference_list, coordinator_in_list)
            
            print(f"[{self.node_id}] Successful writes: {successful_writes}/{NodeConfig.N}")
            
            # Check if we achieved W quorum
            if successful_writes >= NodeConfig.W:
                return dht_pb2.PutResponse(
                    success=True,
                    message=f"Stored with quorum W={NodeConfig.W} ({successful_writes} replicas)",
                    vector_clock=final_clock.to_dict()
                )
            
            # W quorum not achieved with preference list
            # Try sloppy quorum if enabled
            if NodeConfig.ENABLE_HINTED_HANDOFF and successful_writes < NodeConfig.W:
                print(f"[{self.node_id}] W quorum not met, attempting sloppy quorum with hints...")
                
                # Get extended preference list with fallback nodes
                extended_list = self.hash_ring.get_preference_list(
                    key,
                    n=NodeConfig.N + NodeConfig.SLOPPY_QUORUM_EXTRA_NODES,
                    datacenter_aware=NodeConfig.DATACENTER_AWARE,
                    datacenter_map=NodeConfig.DATACENTER_MAP
                )
                
                # Find nodes that failed in the original preference list
                # and fallback nodes we can try
                fallback_nodes = [n for n in extended_list if n not in preference_list]
                failed_preference_nodes = []
                
                # Determine which preference list nodes failed
                # (We'll use hints to track which node each fallback is replacing)
                for node in preference_list:
                    if node != self.address:  # Don't count coordinator
                        # We need to check if this node was successfully written to
                        # For simplicity, we'll create hints for all failed writes
                        pass
                
                # Try fallback nodes with hints until we reach W
                for fallback_node in fallback_nodes:
                    if successful_writes >= NodeConfig.W:
                        break
                    
                    # Find which preference list node this is replacing (for hint)
                    # In a real implementation, we'd track which nodes failed
                    # For now, use a simple heuristic: hint for nodes in preference list
                    for pref_node in preference_list:
                        if pref_node == self.address:
                            continue
                        
                        # Try to write to fallback with hint for pref_node
                        try:
                            success = self._replicate_put_with_hint(
                                key, value, final_clock, 
                                fallback_node, pref_node
                            )
                            
                            if success:
                                successful_writes += 1
                                print(f"[{self.node_id}] Sloppy quorum: wrote to {fallback_node} with hint for {pref_node}")
                                break  # Move to next fallback
                            
                        except Exception as e:
                            print(f"[{self.node_id}] Failed to write hint to {fallback_node}: {e}")
                
                print(f"[{self.node_id}] After sloppy quorum: {successful_writes}/{NodeConfig.N} writes")
            
            # Final check after sloppy quorum attempt
            if successful_writes >= NodeConfig.W:
                return dht_pb2.PutResponse(
                    success=True,
                    message=f"Stored with sloppy quorum W={NodeConfig.W} ({successful_writes} replicas, may include hints)",
                    vector_clock=final_clock.to_dict()
                )
            else:
                return dht_pb2.PutResponse(
                    success=False,
                    message=f"Failed to achieve write quorum W={NodeConfig.W} (only {successful_writes} replicas)",
                    vector_clock=final_clock.to_dict()
                )
                
        except Exception as e:
            print(f"[{self.node_id}] PUT error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return dht_pb2.PutResponse(
                success=False,
                message=f"Error: {str(e)}",
                vector_clock={}
            )
    
    def Get(self, request, context):
        """
        Handle client GET request with quorum reads and conflict resolution.
        Acts as coordinator for this request.
        """
        try:
            key = request.key
            print(f"[{self.node_id}] Client GET: key='{key}'")
            
            preference_list = self.hash_ring.get_preference_list(
                key,
                n=NodeConfig.N,
                datacenter_aware=NodeConfig.DATACENTER_AWARE,
                datacenter_map=NodeConfig.DATACENTER_MAP
            )
            
            print(f"[{self.node_id}] Preference list for '{key}': {preference_list}")
            print(f"[{self.node_id}] Reading from first {NodeConfig.R} nodes in preference list")
            
            # Pass first R nodes from preference list
            # If coordinator is in the list, it will read locally + query R-1 others
            # If coordinator is NOT in the list, it will query all R nodes
            all_versions = self._replicate_get(key, preference_list[:NodeConfig.R])
            
            if not all_versions:
                return dht_pb2.GetResponse(
                    success=False,
                    key=key,
                    value="",
                    message=f"Key '{key}' not found",
                    vector_clock={},
                    has_conflicts=False
                )
            
            latest_versions, conflicts = reconcile_versions(all_versions)
            
            if conflicts:
                print(f"[{self.node_id}] CONFLICT: {len(conflicts)} sibling versions for '{key}'")
                
                sibling_values = [v.value for v in conflicts]
                sibling_clocks = [dht_pb2.VectorClockMsg(clock=v.vector_clock.to_dict()) for v in conflicts]
                
                if NodeConfig.ENABLE_READ_REPAIR and NodeConfig.READ_REPAIR_ASYNC:
                    threading.Thread(
                        target=self._read_repair,
                        args=(key, latest_versions, preference_list)
                    ).start()
                
                return dht_pb2.GetResponse(
                    success=True,
                    key=key,
                    value=sibling_values[0],
                    message=f"Conflict detected: {len(conflicts)} versions. Client must resolve.",
                    vector_clock=conflicts[0].vector_clock.to_dict(),
                    has_conflicts=True,
                    sibling_values=sibling_values,
                    sibling_clocks=sibling_clocks
                )
            else:
                latest = latest_versions[0]
                
                if NodeConfig.ENABLE_READ_REPAIR and not NodeConfig.READ_REPAIR_ASYNC:
                    self._read_repair(key, latest_versions, preference_list)
                
                return dht_pb2.GetResponse(
                    success=True,
                    key=key,
                    value=latest.value,
                    message=f"Retrieved from quorum R={NodeConfig.R}",
                    vector_clock=latest.vector_clock.to_dict(),
                    has_conflicts=False
                )
                
        except Exception as e:
            print(f"[{self.node_id}] GET error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return dht_pb2.GetResponse(
                success=False,
                key=request.key,
                value="",
                message=f"Error: {str(e)}",
                vector_clock={},
                has_conflicts=False
            )
    
    def InternalPut(self, request, context):
        """
        Handle internal PUT request from another node (replication).
        """
        try:
            import os
            import random
            if os.environ.get("DHT_STRAGGLER_DELAY"):
                delay_val = os.environ.get("DHT_STRAGGLER_DELAY")
                # Check if it's a range like "0.05-0.2"
                if '-' in delay_val:
                    min_d, max_d = map(float, delay_val.split('-'))
                    time.sleep(random.uniform(min_d, max_d))
                else:
                    time.sleep(float(delay_val))
            
            key = request.key
            value = request.value
            vector_clock = VectorClock.from_dict(dict(request.vector_clock))
            
            print(f"[{self.node_id}] Internal PUT (replication): key='{key}'")
            
            self.storage.put(key, value, vector_clock, is_replication=True)
            
            return dht_pb2.InternalPutResponse(
                success=True,
                message="Replication successful"
            )
            
        except Exception as e:
            return dht_pb2.InternalPutResponse(
                success=False,
                message=f"Error: {str(e)}"
            )
    
    def InternalGet(self, request, context):
        """
        Handle internal GET request from another node.
        """
        try:
            key = request.key
            versions = self.storage.get(key)
            
            if not versions:
                return dht_pb2.InternalGetResponse(
                    success=False,
                    versions=[]
                )
            
            versions_data = []
            for vv in versions:
                versions_data.append(dht_pb2.VersionedData(
                    value=vv.value,
                    vector_clock=vv.vector_clock.to_dict()
                ))
            
            return dht_pb2.InternalGetResponse(
                success=True,
                versions=versions_data
            )
            
        except Exception as e:
            return dht_pb2.InternalGetResponse(
                success=False,
                versions=[]
            )
    
    def ReadRepair(self, request, context):
        """
        Handle read repair request from coordinator.
        """
        try:
            key = request.key
            versions = []
            
            for version_data in request.versions:
                vc = VectorClock.from_dict(dict(version_data.vector_clock))
                vv = VersionedValue(version_data.value, vc)
                versions.append(vv)
            
            self.storage.merge_versions(key, versions)
            
            return dht_pb2.ReadRepairResponse(
                success=True,
                message="Read repair completed"
            )
            
        except Exception as e:
            return dht_pb2.ReadRepairResponse(
                success=False,
                message=f"Error: {str(e)}"
            )
    
    def InternalPutWithHint(self, request, context):
        """
        Handle internal PUT request with hint (for sloppy quorum).
        Stores data with a hint indicating it belongs to another node.
        """
        try:
            key = request.key
            value = request.value
            vector_clock = VectorClock.from_dict(dict(request.vector_clock))
            hint_for_node = request.hint_for_node
            
            if hint_for_node:
                # This is a hinted replica - store separately
                print(f"[{self.node_id}] Internal PUT with HINT for {hint_for_node}: key='{key}'")
                success = self.storage.put_with_hint(key, value, vector_clock, hint_for_node)
                
                if success:
                    return dht_pb2.InternalPutWithHintResponse(
                        success=True,
                        message=f"Stored as hint for {hint_for_node}"
                    )
                else:
                    return dht_pb2.InternalPutWithHintResponse(
                        success=False,
                        message="Failed to store hint"
                    )
            else:
                # No hint - store normally
                print(f"[{self.node_id}] Internal PUT (no hint): key='{key}'")
                self.storage.put(key, value, vector_clock, is_replication=True)
                
                return dht_pb2.InternalPutWithHintResponse(
                    success=True,
                    message="Stored successfully"
                )
            
        except Exception as e:
            print(f"[{self.node_id}] Error in InternalPutWithHint: {e}")
            return dht_pb2.InternalPutWithHintResponse(
                success=False,
                message=f"Error: {str(e)}"
            )
    
    def DeliverHint(self, request, context):
        """
        Handle hint delivery from another node.
        This is called when a node that was down comes back up,
        and another node needs to transfer hinted data to it.
        """
        try:
            key = request.key
            value = request.value
            vector_clock = VectorClock.from_dict(dict(request.vector_clock))
            
            print(f"[{self.node_id}] Received HINT delivery: key='{key}'")
            
            # Store normally (this data belongs to us now)
            self.storage.put(key, value, vector_clock, is_replication=True)
            
            return dht_pb2.DeliverHintResponse(
                success=True,
                message="Hint delivered successfully"
            )
            
        except Exception as e:
            print(f"[{self.node_id}] Error in DeliverHint: {e}")
            return dht_pb2.DeliverHintResponse(
                success=False,
                message=f"Error: {str(e)}"
            )
    
    def HealthCheck(self, request, context):
        """
        Handle health check request.
        """
        print(f"[{self.node_id}] Health check from {request.node_id}")
        return dht_pb2.HealthCheckResponse(
            healthy=True,
            node_id=self.node_id,
            datacenter=self.datacenter
        )
    
    def GetRing(self, request, context):
        """
        Handle GetRing request - returns all nodes in the ring.
        """
        print(f"[{self.node_id}] GetRing request from {request.requesting_node_id}")
        
        # Get all nodes in the ring
        all_nodes = self.hash_ring.get_all_nodes()
        
        # Build datacenter map
        dc_map = {}
        for node_address in all_nodes:
            dc_map[node_address] = NodeConfig.DATACENTER_MAP.get(node_address, "UNKNOWN")
        
        return dht_pb2.GetRingResponse(
            nodes=all_nodes,
            datacenter_map=dc_map
        )
    
    def JoinCluster(self, request, context):
        """
        Handle JoinCluster request - add a new node to the ring.
        Propagates the new member to other nodes for faster convergence.
        """
        node_address = request.node_address
        datacenter = request.datacenter
        
        print(f"[{self.node_id}] Join request from {node_address} (DC: {datacenter})")
        
        # Add node to ring if not already present
        if node_address not in self.hash_ring.get_all_nodes():
            self.hash_ring.add_node(node_address)
            NodeConfig.add_node_to_datacenter(node_address, datacenter)
            self.hash_ring.save_ring_state(self.node_id)
            print(f"[{self.node_id}] Added {node_address} to ring. Ring now has {len(self.hash_ring.get_all_nodes())} nodes")
            
            # IMPROVEMENT: Propagate membership to random peers for faster convergence
            self._propagate_membership(node_address, datacenter)
            
            return dht_pb2.JoinClusterResponse(
                success=True,
                message=f"Successfully joined cluster. Ring has {len(self.hash_ring.get_all_nodes())} nodes"
            )
        else:
            return dht_pb2.JoinClusterResponse(
                success=True,
                message="Node already in ring"
            )
    
    def _propagate_membership(self, new_node_address: str, datacenter: str):
        """
        Propagate new node membership to random subset of peers.
        Uses fanout of 3 for epidemic-style propagation.
        """
        def propagate():
            current_nodes = self.hash_ring.get_all_nodes()
            if len(current_nodes) <= 2:  # Just us and new node
                return
            
            # Pick 3 random peers (excluding self and new node)
            peers = [n for n in current_nodes if n != self.address and n != new_node_address]
            fanout = min(3, len(peers))
            
            if fanout > 0:
                targets = random.sample(peers, fanout)
                
                for target in targets:
                    try:
                        stub = self._get_node_stub(target)
                        if stub:
                            request = dht_pb2.JoinClusterRequest(
                                node_address=new_node_address,
                                datacenter=datacenter
                            )
                            stub.JoinCluster(request, timeout=2)
                    except:
                        pass  # Silent propagation, best effort
        
        # Run in background thread
        threading.Thread(target=propagate, daemon=True).start()
    
    def _discover_nodes(self):
        """
        Discover all nodes in the cluster by querying known nodes.
        Uses multi-round discovery and queries all sources for better convergence.
        """
        if not self.known_nodes:
            return
        
        print(f"[{self.node_id}] Discovering nodes from known nodes...")
        
        all_discovered = set()
        
        # IMPROVED: Multiple rounds with increasing peers
        max_rounds = 8  # Increased from 3
        
        for round_num in range(max_rounds):
            round_discoveries = 0
            
            # Round 1-2: Query all known seed nodes
            if round_num < 2:
                sources_to_query = list(self.known_nodes)
            else:
                # Round 3+: Query random sample of discovered nodes
                current_nodes = list(self.hash_ring.get_all_nodes())
                sample_size = min(5, len(current_nodes))
                sources_to_query = [n for n in random.sample(current_nodes, sample_size) 
                                   if n != self.address]
            
            # Query EACH source (don't break after first success)
            for source_node in sources_to_query:
                try:
                    stub = self._get_node_stub(source_node)
                    if not stub:
                        continue
                    
                    request = dht_pb2.GetRingRequest(requesting_node_id=self.node_id)
                    response = stub.GetRing(request, timeout=3)
                    
                    for node_address in response.nodes:
                        if node_address != self.address and node_address not in all_discovered:
                            all_discovered.add(node_address)
                            round_discoveries += 1
                            
                            # Add to ring if not already present
                            if node_address not in self.hash_ring.get_all_nodes():
                                self.hash_ring.add_node(node_address)
                            
                            # Update datacenter map
                            if node_address in response.datacenter_map:
                                dc = response.datacenter_map[node_address]
                                NodeConfig.add_node_to_datacenter(node_address, dc)
                    
                except Exception as e:
                    # Silently continue to other sources
                    continue
            
            current_ring_size = len(self.hash_ring.get_all_nodes())
            print(f"[{self.node_id}] Round {round_num + 1}: discovered {round_discoveries} new nodes, "
                  f"ring now has {current_ring_size} nodes")
            
            # If no new discoveries in this round and we have a reasonable number, we're done
            if round_discoveries == 0 and current_ring_size > len(self.known_nodes):
                break
            
            # Wait between rounds for more nodes to join
            if round_num < max_rounds - 1:
                time.sleep(3)  # Increased from 2
        
        # Save updated ring state
        self.hash_ring.save_ring_state(self.node_id)
        print(f"[{self.node_id}] Discovery complete: ring contains {len(self.hash_ring.get_all_nodes())} nodes")

    # Add this to server_v2.py inside class DHTServicer:
    def InspectNode(self, request, context):
        entries = []
        with self.storage.lock:
            for key, versions in self.storage.data.items():
                if versions:
                    # Show latest version
                    latest = versions[-1]
                    entries.append(dht_pb2.StorageEntry(
                        key=key,
                        value=latest.value,
                        version_summary=str(latest.vector_clock)
                    ))
        
        return dht_pb2.InspectNodeResponse(
            success=True,
            entries=entries,
            datacenter=self.datacenter,
            key_count=len(entries)
        )


def serve(node_id: str, host: str, port: int, known_nodes: list = None, 
          datacenter: str = "UNKNOWN"):
    """
    Start the gRPC server with full DHT functionality.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    servicer = DHTServicer(node_id, host, port, known_nodes, datacenter)
    dht_pb2_grpc.add_DHTServiceServicer_to_server(servicer, server)
    
    server_address = f"{host}:{port}"
    server.add_insecure_port(server_address)
    
    server.start()
    print(f"\n{'='*70}")
    print(f"DHT Node '{node_id}' started (Phase 2+3 Features)")
    print(f"Address: {server_address}")
    print(f"Datacenter: {datacenter}")
    print(f"Replication: N={NodeConfig.N}, R={NodeConfig.R}, W={NodeConfig.W}")
    print(f"{'='*70}\n")
    
    NodeConfig.validate_quorum_config()
    
    # Initialize cluster membership (discover nodes from known nodes)
    servicer.initialize_cluster_membership()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n[{node_id}] Shutting down gracefully...")
        server.stop(0)

def main():
    """Main entry point with enhanced argument parsing."""
    parser = argparse.ArgumentParser(description='DHT Node Server (Phases 2+3)')
    parser.add_argument('--node-id', type=str, required=True,
                       help='Unique node identifier')
    parser.add_argument('--host', type=str, default='localhost',
                       help='Host address')
    parser.add_argument('--port', type=int, required=True,
                       help='Port number')
    parser.add_argument('--known-nodes', type=str, nargs='*',
                       help='Known nodes (host:port format)')
    parser.add_argument('--datacenter', type=str, default='UNKNOWN',
                       help='Datacenter ID (e.g., DC1, DC2, DC3)')
    parser.add_argument('--enable-dc-aware', action='store_true',
                       help='Enable datacenter-aware replication')
    parser.add_argument('--n', type=int, default=3,
                       help='Number of replicas (N)')
    parser.add_argument('--r', type=int, default=2,
                       help='Read quorum (R)')
    parser.add_argument('--w', type=int, default=2,
                       help='Write quorum (W)')
    parser.add_argument('--disable-read-repair', action='store_true', 
                        help='Disable read repair')
    parser.add_argument('--virtual-nodes', type=int, default=150, 
                        help='Number of virtual nodes per physical node')
    parser.add_argument('--hint-interval', type=int, default=30, 
                        help='Hint delivery check interval (seconds)')
    
    args = parser.parse_args()
    
    NodeConfig.N = args.n
    NodeConfig.R = args.r
    NodeConfig.W = args.w
    NodeConfig.DATACENTER_AWARE = args.enable_dc_aware
    NodeConfig.ENABLE_READ_REPAIR = not args.disable_read_repair
    NodeConfig.NUM_VIRTUAL_NODES = args.virtual_nodes
    NodeConfig.HINT_DELIVERY_INTERVAL_SEC = args.hint_interval
    
    serve(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        known_nodes=args.known_nodes,
        datacenter=args.datacenter
    )


if __name__ == '__main__':
    main()
