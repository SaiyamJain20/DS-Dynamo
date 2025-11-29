#!/usr/bin/env python3
"""
Comprehensive Test Suite for Distributed Hash Table (DHT)
Tests 100 nodes across 10 datacenters with 5 concurrent clients and 500 items

This test suite covers:
1. Multi-datacenter deployment (10 DCs, 10 nodes each)
2. Concurrent client operations (5 clients)
3. Large-scale data operations (500 items)
4. Replication and consistency (N=3, R=2, W=2)
5. Node failures and recovery
6. Persistence and crash recovery
7. Conflict resolution with vector clocks
8. Read repair mechanisms
9. Datacenter-aware replication
10. Load distribution across nodes
11. Quorum-based operations
12. Version reconciliation
13. Client-side vector clock tracking (prevents false conflicts)
14. Read repair verification and convergence
"""

import grpc
import dht_pb2
import dht_pb2_grpc
import time
import subprocess
import signal
import os
import sys
import json
import threading
import random
from collections import defaultdict
from typing import List, Dict, Set, Tuple
import argparse
from config import NodeConfig


# Test Configuration
NUM_DATACENTERS = 10
NODES_PER_DC = 20
TOTAL_NODES = NUM_DATACENTERS * NODES_PER_DC  # 200 nodes
NUM_CLIENTS = 50
NUM_ITEMS = 5000
BASE_PORT = 50051

# Node tracking
node_processes = []
node_info = []  # List of (node_id, host, port, dc)


class TestStats:
    """Track test statistics"""
    def __init__(self):
        self.lock = threading.Lock()
        self.puts_successful = 0
        self.puts_failed = 0
        self.gets_successful = 0
        self.gets_failed = 0
        self.conflicts_detected = 0
        self.nodes_crashed = 0
        self.nodes_recovered = 0
        self.read_repairs = 0
        
    def record_put_success(self):
        with self.lock:
            self.puts_successful += 1
    
    def record_put_failure(self):
        with self.lock:
            self.puts_failed += 1
    
    def record_get_success(self):
        with self.lock:
            self.gets_successful += 1
    
    def record_get_failure(self):
        with self.lock:
            self.gets_failed += 1
    
    def record_conflict(self):
        with self.lock:
            self.conflicts_detected += 1
    
    def record_crash(self):
        with self.lock:
            self.nodes_crashed += 1
    
    def record_recovery(self):
        with self.lock:
            self.nodes_recovered += 1
    
    def record_read_repair(self):
        with self.lock:
            self.read_repairs += 1
    
    def print_summary(self):
        with self.lock:
            print("\n" + "="*80)
            print("TEST STATISTICS SUMMARY")
            print("="*80)
            print(f"Total PUT Operations:      {self.puts_successful + self.puts_failed}")
            print(f"  ✓ Successful:            {self.puts_successful}")
            print(f"  ✗ Failed:                {self.puts_failed}")
            print(f"Total GET Operations:      {self.gets_successful + self.gets_failed}")
            print(f"  ✓ Successful:            {self.gets_successful}")
            print(f"  ✗ Failed:                {self.gets_failed}")
            print(f"Conflicts Detected:        {self.conflicts_detected}")
            print(f"Nodes Crashed:             {self.nodes_crashed}")
            print(f"Nodes Recovered:           {self.nodes_recovered}")
            print("="*80)


stats = TestStats()


def cleanup_environment():
    """Clean up old data, logs, and processes"""
    print("\n" + "="*80)
    print("PHASE 0: ENVIRONMENT CLEANUP")
    print("="*80)
    
    # Kill any existing server processes
    try:
        subprocess.run(["pkill", "-f", "server_v2.py"], stderr=subprocess.DEVNULL)
        time.sleep(2)
    except:
        pass
    
    # Clean data directory
    if os.path.exists("data"):
        for filename in os.listdir("data"):
            filepath = os.path.join("data", filename)
            try:
                os.remove(filepath)
                print(f"  Removed: {filepath}")
            except Exception as e:
                print(f"  Error removing {filepath}: {e}")
    else:
        os.makedirs("data")
    
    # Clean/create logs directory
    if os.path.exists("logs"):
        for filename in os.listdir("logs"):
            filepath = os.path.join("logs", filename)
            try:
                os.remove(filepath)
            except:
                pass
    else:
        os.makedirs("logs")
    
    print("✓ Environment cleaned")


def start_node(node_id: str, host: str, port: int, dc: str, known_nodes: List[str] = None) -> subprocess.Popen:
    """Start a DHT node"""
    cmd = [
        sys.executable, "server_v2.py",
        "--node-id", node_id,
        "--host", host,
        "--port", str(port),
        "--datacenter", dc,
        "--enable-dc-aware"  # Enable datacenter-aware replication
    ]
    
    if known_nodes:
        for kn in known_nodes:
            cmd.extend(["--known-node", kn])
    
    log_file = open(f"logs/{node_id}.log", "w")
    process = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    return process


def stop_node(process: subprocess.Popen, node_id: str):
    """Stop a DHT node gracefully"""
    if process and process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def get_stub(host: str, port: int):
    """Get gRPC stub for a node"""
    address = f"{host}:{port}"
    channel = grpc.insecure_channel(address)
    return dht_pb2_grpc.DHTServiceStub(channel)


def phase1_deploy_cluster():
    """Phase 1: Deploy 100 nodes across 10 datacenters"""
    print("\n" + "="*80)
    print("PHASE 1: CLUSTER DEPLOYMENT")
    print("="*80)
    print(f"Deploying {TOTAL_NODES} nodes across {NUM_DATACENTERS} datacenters...")
    
    global node_processes, node_info
    
    # First, start the seed node
    seed_dc = "DC1"
    seed_node_id = f"dc1_node1"
    seed_host = "localhost"
    seed_port = BASE_PORT
    seed_address = f"{seed_host}:{seed_port}"
    
    print(f"\n  Starting seed node: {seed_node_id} at {seed_address} ({seed_dc})")
    process = start_node(seed_node_id, seed_host, seed_port, seed_dc)
    node_processes.append(process)
    node_info.append((seed_node_id, seed_host, seed_port, seed_dc))
    time.sleep(2)  # Give seed node time to initialize
    
    # Start remaining nodes
    node_idx = 2
    for dc_num in range(1, NUM_DATACENTERS + 1):
        dc_name = f"DC{dc_num}"
        start_idx = 1 if dc_num == 1 else 1  # Skip first node of DC1 (seed)
        
        for node_num in range(start_idx, NODES_PER_DC + 1):
            if dc_num == 1 and node_num == 1:
                continue  # Skip seed node
            
            node_id = f"dc{dc_num}_node{node_num}"
            host = "localhost"
            port = BASE_PORT + node_idx - 1
            
            # Use seed node as known node for gossip
            known_nodes = [seed_address]
            
            process = start_node(node_id, host, port, dc_name, known_nodes)
            node_processes.append(process)
            node_info.append((node_id, host, port, dc_name))
            
            if node_idx % 10 == 0:
                print(f"  Started {node_idx}/{TOTAL_NODES} nodes...")
            
            node_idx += 1
            time.sleep(0.2)  # Small delay to avoid overwhelming the system
    
    print(f"\n✓ All {TOTAL_NODES} nodes started")
    print(f"  Waiting for cluster stabilization and node discovery...")
    
    # Wait longer for node discovery to complete
    # Each node takes ~1 second to discover, with 20 nodes starting in sequence
    stabilization_time = max(15, TOTAL_NODES // 5)  # At least 15 seconds, or more for large clusters
    
    for i in range(stabilization_time):
        time.sleep(1)
        if (i + 1) % 5 == 0:
            print(f"    {i + 1}/{stabilization_time} seconds...")
    
    # Verify nodes are running
    running = sum(1 for p in node_processes if p.poll() is None)
    print(f"✓ Cluster deployed: {running}/{TOTAL_NODES} nodes running")
    
    if running < TOTAL_NODES:
        print(f"  WARNING: Only {running} nodes running, expected {TOTAL_NODES}")


def phase2_initial_data_load():
    """Phase 2: Load 500 items into the cluster"""
    print("\n" + "="*80)
    print("PHASE 2: INITIAL DATA LOAD")
    print("="*80)
    
    # Verify ring convergence before loading data
    print("Verifying ring convergence...")
    converged_nodes = 0
    for node_id, host, port, dc in random.sample(node_info, min(5, len(node_info))):
        try:
            stub = get_stub(host, port)
            request = dht_pb2.GetRingRequest(requesting_node_id="test")
            response = stub.GetRing(request, timeout=5)
            num_nodes = len(response.nodes)
            converged_nodes += 1
            print(f"  {node_id}: sees {num_nodes} nodes in ring")
        except Exception as e:
            print(f"  {node_id}: ring check failed - {e}")
    
    print(f"  Ring convergence: {converged_nodes}/{min(5, len(node_info))} nodes checked")
    
    print(f"\nLoading {NUM_ITEMS} items into the cluster...")
    
    # Connect to random nodes for load distribution
    successful = 0
    failed = 0
    
    for i in range(NUM_ITEMS):
        key = f"item:{i:04d}"
        value = f"value_{i}_initial"
        
        # Try up to 3 times with different nodes if needed
        max_retries = 1
        success = False
        
        for attempt in range(max_retries):
            # Pick a random node (try different ones on retry)
            node_id, host, port, dc = random.choice(node_info)
            
            try:
                stub = get_stub(host, port)
                request = dht_pb2.PutRequest(key=key, value=value)
                response = stub.Put(request, timeout=5)
                
                if response.success:
                    successful += 1
                    stats.record_put_success()
                    success = True
                    break
                else:
                    # If quorum failure, try another node
                    if attempt < max_retries - 1:
                        time.sleep(0.5)  # Brief pause before retry
                        continue
                    else:
                        failed += 1
                        stats.record_put_failure()
                        if i < 20:  # Only print first 20 failures to avoid spam
                            print(f"  PUT failed for {key}: {response.message}")
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                else:
                    failed += 1
                    stats.record_put_failure()
                    if i < 20:
                        print(f"  Error putting {key}: {e}")
        
        if (i + 1) % 100 == 0:
            print(f"  Loaded {i + 1}/{NUM_ITEMS} items...")
    
    print(f"\n✓ Data load complete: {successful} successful, {failed} failed")
    time.sleep(3)


def phase3_concurrent_client_operations():
    """Phase 3: 5 concurrent clients performing random operations"""
    print("\n" + "="*80)
    print("PHASE 3: CONCURRENT CLIENT OPERATIONS")
    print("="*80)
    print(f"Running {NUM_CLIENTS} concurrent clients...")
    
    def client_worker(client_id: int, num_operations: int):
        """Worker function for each client"""
        operations = ["PUT", "GET", "UPDATE"]
        local_stats = {"put": 0, "get": 0, "update": 0, "errors": 0}
        
        for op_num in range(num_operations):
            operation = random.choice(operations)
            item_id = random.randint(0, NUM_ITEMS - 1)
            key = f"item:{item_id:04d}"
            
            # Pick random node
            node_id, host, port, dc = random.choice(node_info)
            
            try:
                stub = get_stub(host, port)
                
                if operation == "PUT":
                    value = f"value_{item_id}_client{client_id}_op{op_num}"
                    request = dht_pb2.PutRequest(key=key, value=value)
                    response = stub.Put(request, timeout=5)
                    if response.success:
                        local_stats["put"] += 1
                        stats.record_put_success()
                    else:
                        local_stats["errors"] += 1
                        stats.record_put_failure()
                
                elif operation == "GET":
                    request = dht_pb2.GetRequest(key=key)
                    response = stub.Get(request, timeout=5)
                    if response.success:
                        local_stats["get"] += 1
                        stats.record_get_success()
                        if len(response.sibling_values) > 1:
                            stats.record_conflict()
                    else:
                        local_stats["errors"] += 1
                        stats.record_get_failure()
                
                elif operation == "UPDATE":
                    value = f"value_{item_id}_update_client{client_id}_op{op_num}"
                    request = dht_pb2.PutRequest(key=key, value=value)
                    response = stub.Put(request, timeout=5)
                    if response.success:
                        local_stats["update"] += 1
                        stats.record_put_success()
                    else:
                        local_stats["errors"] += 1
                        stats.record_put_failure()
                
                # time.sleep(0.01)  # Small delay between operations
                
            except Exception as e:
                local_stats["errors"] += 1
                if operation in ["PUT", "UPDATE"]:
                    stats.record_put_failure()
                else:
                    stats.record_get_failure()
        
        print(f"  Client {client_id} completed: PUT={local_stats['put']}, "
              f"GET={local_stats['get']}, UPDATE={local_stats['update']}, "
              f"ERRORS={local_stats['errors']}")
    
    # Start client threads
    threads = []
    ops_per_client = 2000
    
    for client_id in range(NUM_CLIENTS):
        thread = threading.Thread(target=client_worker, args=(client_id, ops_per_client))
        thread.start()
        threads.append(thread)
    
    # Wait for all clients to complete
    for thread in threads:
        thread.join()
    
    print(f"\n✓ Concurrent operations complete")
    time.sleep(2)


def phase4_node_failures_and_recovery():
    """Phase 4: Simulate node failures and recovery"""
    print("\n" + "="*80)
    print("PHASE 4: NODE FAILURES AND RECOVERY")
    print("="*80)
    
    # Randomly crash 10 nodes
    num_to_crash = NUM_DATACENTERS * 2  # Crash 20 nodes (4% of cluster)
    print(f"Crashing {num_to_crash} random nodes...")
    
    crashed_nodes = []
    for i in range(num_to_crash):
        node_idx = random.randint(1, len(node_processes) - 1)  # Don't crash seed
        process = node_processes[node_idx]
        node_id, host, port, dc = node_info[node_idx]
        
        if process and process.poll() is None:
            print(f"  Crashing node: {node_id}")
            stop_node(process, node_id)
            crashed_nodes.append((node_idx, node_id, host, port, dc))
            stats.record_crash()
    
    print(f"✓ Crashed {len(crashed_nodes)} nodes")
    print(f"  Waiting 5 seconds...")
    time.sleep(5)
    
    # Perform operations while nodes are down
    print(f"  Testing operations with nodes down...")
    test_ops = 100
    successful = 0
    
    for i in range(test_ops):
        key = f"item:{random.randint(0, NUM_ITEMS-1):04d}"
        value = f"value_during_failure_{i}"
        
        # Try a node that's still running
        node_idx = random.randint(0, len(node_info) - 1)
        while node_idx in [idx for idx, _, _, _, _ in crashed_nodes]:
            node_idx = random.randint(0, len(node_info) - 1)
        
        node_id, host, port, dc = node_info[node_idx]
        
        try:
            stub = get_stub(host, port)
            request = dht_pb2.PutRequest(key=key, value=value)
            response = stub.Put(request, timeout=5)
            if response.success:
                successful += 1
                stats.record_put_success()
        except:
            stats.record_put_failure()
    
    print(f"  Operations during failure: {successful}/{test_ops} successful")
    
    # Recover crashed nodes
    print(f"\n  Recovering {len(crashed_nodes)} crashed nodes...")
    
    for node_idx, node_id, host, port, dc in crashed_nodes:
        # Get seed node for gossip
        seed_address = f"{node_info[0][1]}:{node_info[0][2]}"
        process = start_node(node_id, host, port, dc, [seed_address])
        node_processes[node_idx] = process
        stats.record_recovery()
        print(f"    Recovered: {node_id}")
        time.sleep(0.5)
    
    print(f"✓ All nodes recovered")
    print(f"  Waiting 10 seconds for stabilization...")
    time.sleep(10)


def phase5_consistency_verification():
    """Phase 5: Verify data consistency across replicas"""
    print("\n" + "="*80)
    print("PHASE 5: CONSISTENCY VERIFICATION")
    print("="*80)
    print("Verifying data consistency across replicas...")
    
    # Sample random keys and check from multiple nodes
    sample_size = 200
    consistent = 0
    inconsistent = 0
    not_found = 0
    
    for i in range(sample_size):
        key = f"item:{random.randint(0, NUM_ITEMS-1):04d}"
        
        # Query 5 random nodes
        results = []
        for _ in range(5):
            node_id, host, port, dc = random.choice(node_info)
            try:
                stub = get_stub(host, port)
                request = dht_pb2.GetRequest(key=key)
                response = stub.Get(request, timeout=5)
                if response.success:
                    # Create a simple list with the value (handling both single value and siblings)
                    values = [response.value] if not response.has_conflicts else list(response.sibling_values)
                    results.append((node_id, values))
                    stats.record_get_success()
            except:
                stats.record_get_failure()
        
        if not results:
            not_found += 1
            continue
        
        # Check if all nodes return the same version
        first_values = set(results[0][1])
        all_consistent = all(
            set(values) == first_values 
            for _, values in results
        )
        
        if all_consistent:
            consistent += 1
        else:
            inconsistent += 1
            stats.record_conflict()
    
    print(f"✓ Consistency check complete:")
    print(f"  Consistent: {consistent}/{sample_size}")
    print(f"  Inconsistent: {inconsistent}/{sample_size}")
    print(f"  Not found: {not_found}/{sample_size}")


def phase6_load_distribution_analysis():
    """Phase 6: Analyze load distribution across nodes"""
    print("\n" + "="*80)
    print("PHASE 6: LOAD DISTRIBUTION ANALYSIS")
    print("="*80)
    print("Analyzing data distribution across nodes...")
    
    # Check data files for each node
    dc_distribution = defaultdict(int)
    node_distribution = {}
    total_keys = 0
    
    for node_id, host, port, dc in node_info:
        data_file = f"data/{node_id}_data.json"
        if os.path.exists(data_file):
            try:
                with open(data_file, 'r') as f:
                    data = json.load(f)
                    num_keys = len(data)
                    node_distribution[node_id] = num_keys
                    dc_distribution[dc] += num_keys
                    total_keys += num_keys
            except:
                node_distribution[node_id] = 0
        else:
            node_distribution[node_id] = 0
    
    # Print distribution statistics
    print(f"\nTotal keys stored (with replication): {total_keys}")
    print(f"Average keys per node: {total_keys/TOTAL_NODES:.2f}")
    print(f"\nDatacenter Distribution:")
    for dc in sorted(dc_distribution.keys()):
        print(f"  {dc}: {dc_distribution[dc]} keys")
    
    # Show top 10 most loaded nodes
    sorted_nodes = sorted(node_distribution.items(), key=lambda x: x[1], reverse=True)
    print(f"\nTop 10 Most Loaded Nodes:")
    for i, (node_id, count) in enumerate(sorted_nodes[:10], 1):
        print(f"  {i}. {node_id}: {count} keys")
    
    # Show node load variance
    values = list(node_distribution.values())
    if values:
        avg = sum(values) / len(values)
        variance = sum((x - avg) ** 2 for x in values) / len(values)
        std_dev = variance ** 0.5
        print(f"\nLoad Distribution Statistics:")
        print(f"  Min: {min(values)}")
        print(f"  Max: {max(values)}")
        print(f"  Average: {avg:.2f}")
        print(f"  Std Dev: {std_dev:.2f}")


def phase7_persistence_verification():
    """Phase 7: Verify persistence by restarting nodes"""
    print("\n" + "="*80)
    print("PHASE 7: PERSISTENCE VERIFICATION")
    print("="*80)
    print("Testing persistence by restarting random nodes...")
    
    # Pick 5 random nodes to restart
    num_to_restart = 5
    restart_nodes = []
    
    for i in range(num_to_restart):
        node_idx = random.randint(1, len(node_processes) - 1)
        node_id, host, port, dc = node_info[node_idx]
        
        # Check current data
        data_file = f"data/{node_id}_data.json"
        keys_before = set()
        
        if os.path.exists(data_file):
            try:
                with open(data_file, 'r') as f:
                    data = json.load(f)
                    keys_before = set(data.keys())
            except:
                pass
        
        print(f"\n  Testing node: {node_id} ({len(keys_before)} keys)")
        
        # Stop node
        process = node_processes[node_idx]
        if process and process.poll() is None:
            stop_node(process, node_id)
            print(f"    Stopped node")
            time.sleep(2)
        
        # Restart node
        seed_address = f"{node_info[0][1]}:{node_info[0][2]}"
        new_process = start_node(node_id, host, port, dc, [seed_address])
        node_processes[node_idx] = new_process
        print(f"    Restarted node")
        time.sleep(3)
        
        # Check data after restart
        keys_after = set()
        if os.path.exists(data_file):
            try:
                with open(data_file, 'r') as f:
                    data = json.load(f)
                    keys_after = set(data.keys())
            except:
                pass
        
        # Verify data persisted
        if keys_before == keys_after:
            print(f"    ✓ All {len(keys_after)} keys persisted correctly")
            restart_nodes.append((node_id, True, len(keys_after)))
        else:
            print(f"    ✗ Data mismatch: {len(keys_before)} -> {len(keys_after)}")
            restart_nodes.append((node_id, False, len(keys_after)))
    
    successful_restarts = sum(1 for _, success, _ in restart_nodes if success)
    print(f"\n✓ Persistence test complete: {successful_restarts}/{num_to_restart} nodes verified")


def phase8_stress_test():
    """Phase 8: High-frequency stress test"""
    print("\n" + "="*80)
    print("PHASE 8: STRESS TEST")
    print("="*80)
    print("Running high-frequency stress test...")
    
    def stress_worker(worker_id: int, duration_seconds: int):
        """High-frequency random operations"""
        end_time = time.time() + duration_seconds
        ops_count = 0
        
        while time.time() < end_time:
            key = f"item:{random.randint(0, NUM_ITEMS-1):04d}"
            operation = random.choice(["PUT", "GET"])
            
            node_id, host, port, dc = random.choice(node_info)
            
            try:
                stub = get_stub(host, port)
                
                if operation == "PUT":
                    value = f"stress_{worker_id}_{ops_count}"
                    request = dht_pb2.PutRequest(key=key, value=value)
                    response = stub.Put(request, timeout=2)
                    if response.success:
                        stats.record_put_success()
                else:
                    request = dht_pb2.GetRequest(key=key)
                    response = stub.Get(request, timeout=2)
                    if response.success:
                        stats.record_get_success()
                
                ops_count += 1
            except:
                if operation == "PUT":
                    stats.record_put_failure()
                else:
                    stats.record_get_failure()
        
        print(f"  Stress worker {worker_id}: {ops_count} operations")
    
    # Run stress test for 30 seconds with 10 workers
    duration = 30
    num_workers = 10
    
    print(f"  Running {num_workers} workers for {duration} seconds...")
    
    threads = []
    for worker_id in range(num_workers):
        thread = threading.Thread(target=stress_worker, args=(worker_id, duration))
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()
    
    print(f"✓ Stress test complete")


def phase9_datacenter_awareness_test():
    """Phase 9: Test datacenter-aware operations"""
    print("\n" + "="*80)
    print("PHASE 9: DATACENTER AWARENESS TEST")
    print("="*80)
    print("Testing datacenter-aware replication...")
    
    # Group nodes by datacenter
    dc_nodes = defaultdict(list)
    for node_id, host, port, dc in node_info:
        dc_nodes[dc].append((node_id, host, port))
    
    print(f"  Datacenter node distribution:")
    for dc, nodes in sorted(dc_nodes.items()):
        print(f"    {dc}: {len(nodes)} nodes")
    
    # Test operations from each datacenter
    # Use UNIQUE keys per datacenter to avoid conflicting writes
    all_test_keys = []
    
    for dc_idx, dc in enumerate(sorted(dc_nodes.keys())[:3], 1):  # Test first 3 DCs
        if not dc_nodes[dc]:
            continue
        
        # Create unique keys for this datacenter to avoid conflicts
        test_keys = [f"dc_test_{dc}:{i}" for i in range(20)]
        all_test_keys.extend(test_keys)
        
        node_id, host, port = dc_nodes[dc][0]
        
        print(f"\n  Testing from {dc} ({node_id})...")
        successful = 0
        
        for key in test_keys:
            value = f"value_from_{dc}"
            try:
                stub = get_stub(host, port)
                request = dht_pb2.PutRequest(key=key, value=value)
                response = stub.Put(request, timeout=5)
                if response.success:
                    successful += 1
                    stats.record_put_success()
            except:
                stats.record_put_failure()
        
        print(f"    Operations: {successful}/{len(test_keys)} successful")
    
    # Wait for replication to complete
    print(f"\n  Waiting for replication to complete...")
    time.sleep(5)
    
    # Check replica distribution across datacenters
    print(f"\n  Verifying replica distribution across datacenters...")
    
    # Create a map of node_id to datacenter
    node_to_dc = {}
    for node_id, host, port, dc in node_info:
        node_to_dc[node_id] = dc
    
    # Sample some keys from each DC to check their replica distribution
    sample_keys = []
    for dc_idx, dc in enumerate(sorted(dc_nodes.keys())[:3], 1):
        if dc_nodes[dc]:
            # Sample 2 keys from each datacenter
            sample_keys.extend([f"dc_test_{dc}:0", f"dc_test_{dc}:10"])
    
    cross_dc_replicas = 0
    same_dc_replicas = 0
    keys_checked = 0
    exact_n_replicas = 0
    
    for key in sample_keys:
        # Find which nodes have this key
        nodes_with_key = []
        
        for node_id, host, port, dc in node_info:
            data_file = f"data/{node_id}_data.json"
            if os.path.exists(data_file):
                try:
                    with open(data_file, 'r') as f:
                        data = json.load(f)
                        if key in data:
                            nodes_with_key.append((node_id, dc))
                except:
                    pass
        
        if len(nodes_with_key) >= 2:
            keys_checked += 1
            num_replicas = len(nodes_with_key)
            
            # Check if we have exactly N replicas
            if num_replicas == NodeConfig.N:
                exact_n_replicas += 1
            
            # Get unique datacenters holding this key
            dcs_with_key = set(dc for _, dc in nodes_with_key)
            
            # Check if replicas are in different datacenters
            if len(dcs_with_key) > 1:
                cross_dc_replicas += 1
                status = "✓" if num_replicas == NodeConfig.N else "⚠"
                print(f"    {status} Key '{key}': {num_replicas} replicas (N={NodeConfig.N}) across {len(dcs_with_key)} DCs")
                print(f"      Nodes: {', '.join(f'{nid}({dc})' for nid, dc in nodes_with_key)}")
            else:
                same_dc_replicas += 1
                status = "⚠" 
                print(f"    {status} Key '{key}': {num_replicas} replicas (N={NodeConfig.N}) in SAME DC ({dcs_with_key.pop()})")
                print(f"      Nodes: {', '.join(f'{nid}' for nid, _ in nodes_with_key)}")
        elif len(nodes_with_key) == 1:
            print(f"    ⚠ Key '{key}': Only {len(nodes_with_key)} replica found (expected N={NodeConfig.N})")
        else:
            print(f"    ✗ Key '{key}': No replicas found!")
    
    if keys_checked > 0:
        cross_dc_percentage = (cross_dc_replicas / keys_checked) * 100
        exact_n_percentage = (exact_n_replicas / keys_checked) * 100
        print(f"\n  Replica Distribution Summary:")
        print(f"    Keys with cross-DC replicas:  {cross_dc_replicas}/{keys_checked} ({cross_dc_percentage:.1f}%)")
        print(f"    Keys with same-DC replicas:   {same_dc_replicas}/{keys_checked}")
        print(f"    Keys with exactly N replicas: {exact_n_replicas}/{keys_checked} ({exact_n_percentage:.1f}%)")
        
        if cross_dc_replicas > 0:
            print(f"    ✓ System is distributing replicas across datacenters")
        else:
            print(f"    ⚠ System may not be datacenter-aware")
        
        if exact_n_replicas == keys_checked:
            print(f"    ✓ All keys have exactly N={NodeConfig.N} replicas (no over-replication)")
        elif exact_n_replicas < keys_checked:
            print(f"    ⚠ Some keys have more/fewer than N={NodeConfig.N} replicas (possible conflicts or replication issues)")
    
    print(f"\n✓ Datacenter awareness test complete")


def phase10_vector_clock_tracking_test():
    """Phase 10: Test client-side vector clock tracking (prevents false conflicts)"""
    print("\n" + "="*80)
    print("PHASE 10: CLIENT-SIDE VECTOR CLOCK TRACKING TEST")
    print("="*80)
    
    from client_v2 import DHTClient
    
    # If node_info is empty (running standalone), use default cluster
    if not node_info:
        sample_nodes = [f'localhost:5005{i}' for i in range(1, 7)]
    else:
        sample_nodes = []
        for i in range(min(6, len(node_info))):
            node_id, host, port, dc = node_info[i]
            sample_nodes.append(f"{host}:{port}")
    
    try:
        client = DHTClient(sample_nodes)
    except Exception as e:
        print(f"✗ Failed to create client: {e}")
        return
    
    # Test 1: Sequential writes to same key with same client
    print(f"\n  Test 1: Sequential writes (no false conflicts)")
    test_key = "vector_clock_test_1"
    
    result1 = client.put(test_key, 'value1', max_retries=1)
    if not result1 or not result1.success:
        print(f"    ✗ Write 1 failed")
        return

    time.sleep(0.1)

    result2 = client.put(test_key, 'value2', max_retries=1)
    if not result2 or not result2.success:
        print(f"    ✗ Write 2 failed")
        return

    time.sleep(0.1)

    result3 = client.get(test_key, max_retries=1)
    if not result3 or not result3.success:
        print(f"    ✗ GET failed")
        return
    
    if result3.has_conflicts:
        print(f"    ✗ FALSE CONFLICT DETECTED - {len(result3.sibling_values)} versions")
        stats.record_get_failure()
    else:
        print(f"    ✓ No conflicts, value: '{result3.value}'")
        stats.record_get_success()
    
    # Test 2: Multiple sequential writes
    print(f"\n  Test 2: Multiple sequential writes")
    test_key2 = "vector_clock_test_2"
    
    for i in range(1, 4):
        result = client.put(test_key2, f'version{i}', max_retries=1)
        if result and result.success:
            stats.record_put_success()
        else:
            stats.record_put_failure()
        time.sleep(0.1)
    
    time.sleep(0.1)
    
    result = client.get(test_key2, max_retries=1)
    if result and result.success:
        if result.has_conflicts:
            print(f"    ✗ FALSE CONFLICT - {len(result.sibling_values)} versions")
            stats.record_get_failure()
        else:
            print(f"    ✓ No conflicts, latest value: '{result.value}'")
            stats.record_get_success()
    
    # Test 3: Read-then-write pattern
    print(f"\n  Test 3: Read-then-write pattern")
    test_key3 = "vector_clock_test_3"
    
    result = client.put(test_key3, 'initial', max_retries=1)
    if result and result.success:
        stats.record_put_success()
    
    time.sleep(0.1)
    
    result = client.get(test_key3, max_retries=1)
    if result and result.success:
        stats.record_get_success()
    
    time.sleep(0.1)
    
    result = client.put(test_key3, 'updated', max_retries=1)
    if result and result.success:
        print(f"    ✓ Update successful with cached clock")
        stats.record_put_success()
    
    print(f"\n✓ Vector clock tracking test complete")



def phase11_read_repair_test():
    """Phase 11: Test read repair functionality"""
    print("\n" + "="*80)
    print("PHASE 11: READ REPAIR TEST")
    print("="*80)
    
    # If node_info is empty (running standalone), use default cluster
    if not node_info:
        test_nodes = [
            ("dc1_node1", "localhost", 50051, "DC1"),
            ("dc1_node2", "localhost", 50052, "DC1"),
            ("dc2_node1", "localhost", 50053, "DC2")
        ]
    else:
        if len(node_info) < 3:
            print("  ⚠ Not enough nodes for read repair test")
            return
        test_nodes = [node_info[i] for i in range(3)]
    
    node1_id, host1, port1, dc1 = test_nodes[0]
    node2_id, host2, port2, dc2 = test_nodes[1]
    node3_id, host3, port3, dc3 = test_nodes[2]
    
    print(f"  Using nodes: {node1_id}, {node2_id}, {node3_id}")
    
    # Step 1: Write a key
    test_key = "read_repair_test"
    print(f"\n  Step 1: Initial write via {node1_id}")
    
    try:
        stub1 = get_stub(host1, port1)
        request = dht_pb2.PutRequest(key=test_key, value='v1')
        response = stub1.Put(request, timeout=5)
        if response.success:
            initial_clock = response.vector_clock
            print(f"    ✓ Write successful")
            stats.record_put_success()
        else:
            print(f"    ✗ Write failed")
            stats.record_put_failure()
            return
    except Exception as e:
        print(f"    ✗ Error: {e}")
        stats.record_put_failure()
        return

    time.sleep(0.1)

    # Step 2: Update value via different node
    print(f"\n  Step 2: Update via {node2_id}")
    
    try:
        stub2 = get_stub(host2, port2)
        request = dht_pb2.PutRequest(key=test_key, value='v2', vector_clock=initial_clock)
        response = stub2.Put(request, timeout=5)
        if response.success:
            print(f"    ✓ Update successful")
            stats.record_put_success()
        else:
            print(f"    ✗ Update failed")
            stats.record_put_failure()
            return
    except Exception as e:
        print(f"    ✗ Error: {e}")
        stats.record_put_failure()
        return
    
    time.sleep(1)
    
    # Step 3: Read from third node (triggers read repair)
    print(f"\n  Step 3: Read from {node3_id} (triggers repair)")
    
    try:
        stub3 = get_stub(host3, port3)
        request = dht_pb2.GetRequest(key=test_key)
        response = stub3.Get(request, timeout=5)
        if response.success:
            if response.has_conflicts:
                print(f"    ✓ Read triggered (conflicts detected)")
            else:
                print(f"    ✓ Read successful, value: '{response.value}'")
            stats.record_get_success()
        else:
            print(f"    ✗ Read failed")
            stats.record_get_failure()
            return
    except Exception as e:
        print(f"    ✗ Error: {e}")
        stats.record_get_failure()
        return
    
    # Step 4: Wait for read repair
    print(f"\n  Step 4: Waiting for repair propagation...")
    time.sleep(3)
    
    # Step 5: Verify consistency across all nodes
    print(f"\n  Step 5: Verify consistency")
    
    values = []
    
    for node_id, host, port, dc in test_nodes:
        try:
            stub = get_stub(host, port)
            request = dht_pb2.GetRequest(key=test_key)
            response = stub.Get(request, timeout=5)
            if response.success and not response.has_conflicts:
                values.append(response.value)
            stats.record_get_success()
        except:
            stats.record_get_failure()
    
    # Check if all nodes have the same value
    if len(values) >= 2:
        unique_values = set(values)
        if len(unique_values) == 1:
            print(f"    ✓ All replicas consistent: value='{values[0]}'")
        else:
            print(f"    ⚠ Different values found: {unique_values}")
    
    print(f"\n✓ Read repair test complete")


def cleanup_cluster():
    """Cleanup: Stop all nodes"""
    print("\n" + "="*80)
    print("CLEANUP: STOPPING ALL NODES")
    print("="*80)
    
    stopped = 0
    for i, process in enumerate(node_processes):
        if process and process.poll() is None:
            node_id = node_info[i][0] if i < len(node_info) else f"node_{i}"
            stop_node(process, node_id)
            stopped += 1
    
    print(f"✓ Stopped {stopped} nodes")


def main():
    """Main test execution"""
    parser = argparse.ArgumentParser(description='Comprehensive DHT Test Suite')
    parser.add_argument('--skip-cleanup', action='store_true', help='Skip initial cleanup')
    parser.add_argument('--phases', type=str, help='Comma-separated phase numbers to run (e.g., "1,2,3")')
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("COMPREHENSIVE DHT TEST SUITE")
    print("="*80)
    print(f"Configuration:")
    print(f"  Nodes: {TOTAL_NODES} ({NODES_PER_DC} per datacenter)")
    print(f"  Datacenters: {NUM_DATACENTERS}")
    print(f"  Concurrent Clients: {NUM_CLIENTS}")
    print(f"  Total Items: {NUM_ITEMS}")
    print(f"  Base Port: {BASE_PORT}")
    print("="*80)
    
    start_time = time.time()
    
    try:
        if not args.skip_cleanup:
            cleanup_environment()
        
        phases_to_run = None
        if args.phases:
            phases_to_run = set(int(p) for p in args.phases.split(','))
        
        if phases_to_run is None or 1 in phases_to_run:
            phase1_deploy_cluster()
        
        if phases_to_run is None or 2 in phases_to_run:
            phase2_initial_data_load()
        
        if phases_to_run is None or 3 in phases_to_run:
            phase3_concurrent_client_operations()
        
        if phases_to_run is None or 4 in phases_to_run:
            phase4_node_failures_and_recovery()
        
        if phases_to_run is None or 5 in phases_to_run:
            phase5_consistency_verification()
        
        if phases_to_run is None or 6 in phases_to_run:
            phase6_load_distribution_analysis()
        
        if phases_to_run is None or 7 in phases_to_run:
            phase7_persistence_verification()
        
        if phases_to_run is None or 8 in phases_to_run:
            phase8_stress_test()
        
        if phases_to_run is None or 9 in phases_to_run:
            phase9_datacenter_awareness_test()
        
        if phases_to_run is None or 10 in phases_to_run:
            phase10_vector_clock_tracking_test()
        
        if phases_to_run is None or 11 in phases_to_run:
            phase11_read_repair_test()
        
    except KeyboardInterrupt:
        print("\n\n⚠ Test interrupted by user")
    except Exception as e:
        print(f"\n\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup_cluster()
        
        elapsed = time.time() - start_time
        print(f"\n" + "="*80)
        print(f"TOTAL TEST DURATION: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        print("="*80)
        
        stats.print_summary()
        
        print("\n" + "="*80)
        print("TEST COMPLETE")
        print("="*80)


if __name__ == "__main__":
    main()
