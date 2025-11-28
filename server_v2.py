"""
DHT Node Server Implementation - Phases 2 & 3
Complete implementation with replication, versioning, and datacenter awareness
"""

import grpc
from concurrent import futures
import argparse
import sys
import time
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
        
        self.storage = NodeStorage(node_id)
        
        self.hash_ring = ConsistentHash(num_virtual_nodes=NodeConfig.NUM_VIRTUAL_NODES)
        
        self.hash_ring.add_node(self.address)
        NodeConfig.add_node_to_datacenter(self.address, datacenter)
        
        self.known_nodes: List[str] = []
        self.node_stubs: Dict[str, dht_pb2_grpc.DHTServiceStub] = {}
        
        if known_nodes:
            for node in known_nodes:
                if node != self.address:
                    self.hash_ring.add_node(node)
                    self.known_nodes.append(node)
        
        print(f"[{self.node_id}] Initialized at {self.address} (DC: {datacenter})")
        print(f"[{self.node_id}] Known nodes: {self.hash_ring.get_all_nodes()}")
        print(f"[{self.node_id}] Replication: N={NodeConfig.N}, R={NodeConfig.R}, W={NodeConfig.W}")
        print(f"[{self.node_id}] Datacenter-aware: {NodeConfig.DATACENTER_AWARE}")
    
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
    
    def _replicate_put(self, key: str, value: str, vector_clock: VectorClock, 
                       replica_nodes: List[str]) -> int:
        """
        Replicate a PUT operation to replica nodes using parallel replication.
        Returns as soon as W-1 additional writes succeed (W-quorum optimization).
        Remaining replications continue in background.
        """
        successful_writes = 1  # Count coordinator's write
        required_additional_writes = NodeConfig.W - 1  # Need W-1 more for quorum
        
        # Filter out self from replica nodes
        target_nodes = [node for node in replica_nodes if node != self.address]
        
        if not target_nodes:
            return successful_writes
        
        # If W=1, coordinator write is enough, start background replication
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
        """
        all_versions = []
        
        local_versions = self.storage.get(key)
        if local_versions:
            all_versions.extend(local_versions)
        
        for node_address in replica_nodes:
            if node_address == self.address:
                continue
            
            try:
                stub = self._get_node_stub(node_address)
                if not stub:
                    continue
                
                request = dht_pb2.InternalGetRequest(key=key)
                response = stub.InternalGet(request, timeout=NodeConfig.REQUEST_TIMEOUT_MS/1000)
                
                if response.success:
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
            
            final_clock = self.storage.put(key, value, vector_clock)
            
            successful_writes = self._replicate_put(key, value, final_clock, preference_list)
            
            print(f"[{self.node_id}] Successful writes: {successful_writes}/{NodeConfig.N}")
            
            if successful_writes >= NodeConfig.W:
                return dht_pb2.PutResponse(
                    success=True,
                    message=f"Stored with quorum W={NodeConfig.W} ({successful_writes} replicas)",
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
            
            print(f"[{self.node_id}] Reading from preference list: {preference_list[:NodeConfig.R]}")
            
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


def serve(node_id: str, host: str, port: int, known_nodes: list = None, 
          datacenter: str = "UNKNOWN"):
    """
    Start the gRPC server with full DHT functionality.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    dht_pb2_grpc.add_DHTServiceServicer_to_server(
        DHTServicer(node_id, host, port, known_nodes, datacenter),
        server
    )
    
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
    
    args = parser.parse_args()
    
    NodeConfig.N = args.n
    NodeConfig.R = args.r
    NodeConfig.W = args.w
    NodeConfig.DATACENTER_AWARE = args.enable_dc_aware
    
    serve(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        known_nodes=args.known_nodes,
        datacenter=args.datacenter
    )


if __name__ == '__main__':
    main()
