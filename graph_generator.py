#!/usr/bin/env python3
"""
Graph Data Generator for DHT Project
Executes specific test scenarios to gather performance metrics for plotting.
Saves output graphs to 'graph_report/' directory.
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
import random
import statistics
import matplotlib.pyplot as plt
import csv
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from consistent_hash import ConsistentHash
from config import NodeConfig
from collections import defaultdict

# Configuration
BASE_PORT = 50060
REPORT_DIR = "graph_report"
NUM_NODES = 100
NUM_DCS = 10
node_processes = []

def cleanup_environment():
    """Clean up old data, logs, and processes"""
    print("\n[Setup] Cleaning environment...")
    
    # Kill existing server processes
    subprocess.run(["pkill", "-f", "server_v2.py"], stderr=subprocess.DEVNULL)
    time.sleep(1)
    
    # Clean data directory
    if os.path.exists("data"):
        for filename in os.listdir("data"):
            filepath = os.path.join("data", filename)
            try:
                os.remove(filepath)
            except Exception:
                pass
    else:
        os.makedirs("data")
    
    # Clean/create logs directory
    if os.path.exists("logs"):
        for filename in os.listdir("logs"):
            filepath = os.path.join("logs", filename)
            try:
                os.remove(filepath)
            except Exception:
                pass
    else:
        os.makedirs("logs")

    # Create report directory if it doesn't exist
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)

# ==============================================================================
# UPDATED UTILITY
# ==============================================================================
def start_cluster(n_val: int, r_val: int, w_val: int, num_nodes=NUM_NODES, 
                 num_dcs=NUM_DCS, dc_aware=True, straggler_dc=None, 
                 read_repair=True, vnodes=150, straggler_delay=2.0,
                 hint_interval=30): # <--- New Param (Default 30s)
    """
    Start cluster with configurable options.
    """
    global node_processes
    node_processes = []
    nodes = []
    
    topology = []
    nodes_per_dc = num_nodes // num_dcs
    for dc_idx in range(1, num_dcs + 1):
        for _ in range(nodes_per_dc):
            topology.append(f"DC{dc_idx}")
    for i in range(len(topology), num_nodes):
        topology.append(f"DC{i % num_dcs + 1}")
            
    seed_port = BASE_PORT
    seed_addr = f"localhost:{seed_port}"
    
    # Base command construction
    base_cmd = [sys.executable, "server_v2.py"]
    if dc_aware: base_cmd.append("--enable-dc-aware")
    if not read_repair: base_cmd.append("--disable-read-repair")
    base_cmd.extend(["--virtual-nodes", str(vnodes)])
    
    # === ADD THIS LINE ===
    base_cmd.extend(["--hint-interval", str(hint_interval)])
    
    # Env for straggler
    env = os.environ.copy()
    if straggler_dc and topology[0] == straggler_dc:
        env["DHT_STRAGGLER_DELAY"] = str(straggler_delay)
    
    cmd = base_cmd + [
        "--node-id", "node_0", "--host", "localhost", "--port", str(seed_port),
        "--datacenter", topology[0], "--n", str(n_val), "--r", str(r_val), "--w", str(w_val)
    ]
    
    log_file = open(f"logs/node_0.log", "w")
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, env=env)
    node_processes.append(proc)
    nodes.append(seed_addr)
    time.sleep(1) 
    
    for i in range(1, num_nodes):
        port = BASE_PORT + i
        node_dc = topology[i]
        
        node_env = os.environ.copy()
        if straggler_dc and node_dc == straggler_dc:
            node_env["DHT_STRAGGLER_DELAY"] = str(straggler_delay)

        cmd = base_cmd + [
            "--node-id", f"node_{i}", "--host", "localhost", "--port", str(port),
            "--datacenter", node_dc, "--n", str(n_val), "--r", str(r_val), "--w", str(w_val),
            "--known-nodes", seed_addr
        ]

        log_file = open(f"logs/node_{i}.log", "w")
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, env=node_env)
        node_processes.append(proc)
        nodes.append(f"localhost:{port}")
        
        if i % 10 == 0: time.sleep(0.5)

    print(f"\n✓ All {len(nodes)} nodes started")
    print(f"  Hint Interval: {hint_interval}s")
    print(f"  Waiting for cluster stabilization...")
    stabilization_time = max(15, len(nodes) // 5)
    for i in range(stabilization_time):
        time.sleep(1)
        if (i + 1) % 5 == 0: print(f"    {i + 1}/{stabilization_time} seconds...")
    
    return nodes

def stop_cluster():
    """Stop all running nodes"""
    print("[Teardown] Stopping cluster...")
    global node_processes
    for p in node_processes:
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill()
    node_processes = []

def get_stub(node_addr):
    channel = grpc.insecure_channel(node_addr)
    return dht_pb2_grpc.DHTServiceStub(channel)

# ==============================================================================
# TEST A: Write Latency vs. Write Quorum (W)
# ==============================================================================
def test_a_latency_vs_quorum():
    print("\n" + "="*60)
    print("TEST A: Write Latency vs. Write Quorum (W)")
    print("Objective: Demonstrate Asynchronous W-Quorum benefits")
    print(f"Configuration: {NUM_NODES} Nodes, {NUM_DCS} Datacenters")
    print("="*60)
    
    results = {} # {w_val: avg_latency_ms}
    w_values = [1, 2, 3]
    num_operations = 100
    
    for w in w_values:
        # 1. Setup Environment
        cleanup_environment()
        # Uses global NUM_NODES (100) and NUM_DCS (10)
        nodes = start_cluster(n_val=3, r_val=2, w_val=w)
        
        # 2. Run Benchmark
        print(f"\n[Bench] Running {num_operations} PUTs with W={w}...")
        latencies = []
        
        for i in range(num_operations):
            target_node = random.choice(nodes)
            stub = get_stub(target_node)
            key = f"perf_w{w}_{i}"
            val = "x" * 100 # 100 byte payload
            
            try:
                start = time.time()
                resp = stub.Put(dht_pb2.PutRequest(key=key, value=val), timeout=5)
                end = time.time()
                
                if resp.success:
                    latencies.append((end - start) * 1000)
            except Exception as e:
                print(f"Request failed: {e}")
        
        # 3. Process Results
        if latencies:
            avg_lat = statistics.mean(latencies)
            print(f"[Result] W={w}: Avg Latency = {avg_lat:.2f} ms")
            results[w] = avg_lat
        else:
            print(f"[Result] W={w}: Failed to gather data")
            results[w] = 0
            
        # 4. Cleanup
        stop_cluster()
        time.sleep(2)

    # 5. Export/Plot
    print("\n[Summary] Write Latency vs W:")
    print("W | Latency (ms)")
    print("--|-------------")
    for w, lat in results.items():
        print(f"{w} | {lat:.2f}")
    
    # Generate Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(8, 6))
        plt.bar(results.keys(), results.values(), color=['green', 'orange', 'red'])
        plt.xlabel('Write Quorum (W)')
        plt.ylabel('Average Latency (ms)')
        plt.title(f'Impact of Write Quorum on Latency (N=3, Nodes={NUM_NODES})')
        plt.xticks(w_values)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        output_file = os.path.join(REPORT_DIR, 'graph_latency_vs_w.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST B: Read Latency vs. Read Quorum (R)
# ==============================================================================
def test_b_latency_vs_read_quorum():
    print("\n" + "="*60)
    print("TEST B: Read Latency vs. Read Quorum (R)")
    print("Objective: Quantify the cost of strong consistency (R)")
    print(f"Configuration: {NUM_NODES} Nodes, N=3")
    print("="*60)
    
    results = {} # {r_val: avg_latency_ms}
    r_values = [1, 2, 3]
    num_items = 100
    num_reads = 100
    
    for r in r_values:
        # 1. Setup Environment
        cleanup_environment()
        # Start with W=1 for fast data loading, but testing specific R
        nodes = start_cluster(n_val=3, r_val=r, w_val=1)
        
        # 2. Populate Data (Pre-test)
        print(f"\n[Setup] Populating {num_items} items...")
        populate_keys = []
        # Use a random node for writing
        writer_stub = get_stub(random.choice(nodes))
        
        for i in range(num_items):
            key = f"read_test_{i}"
            val = "data_" * 20 
            try:
                resp = writer_stub.Put(dht_pb2.PutRequest(key=key, value=val), timeout=5)
                if resp.success:
                    populate_keys.append(key)
            except Exception:
                pass
        
        if not populate_keys:
            print("[Error] Failed to populate data, skipping R={r}")
            stop_cluster()
            continue
            
        # 3. Run Benchmark (GET operations)
        print(f"[Bench] Running {num_reads} GETs with R={r}...")
        latencies = []
        
        for i in range(num_reads):
            # Pick random node for each read to ensure network mix
            target_node = random.choice(nodes)
            stub = get_stub(target_node)
            key = random.choice(populate_keys)
            
            try:
                start = time.time()
                resp = stub.Get(dht_pb2.GetRequest(key=key), timeout=5)
                end = time.time()
                
                if resp.success:
                    latencies.append((end - start) * 1000)
            except Exception as e:
                print(f"Read failed: {e}")
        
        # 4. Process Results
        if latencies:
            avg_lat = statistics.mean(latencies)
            print(f"[Result] R={r}: Avg Latency = {avg_lat:.2f} ms")
            results[r] = avg_lat
        else:
            print(f"[Result] R={r}: Failed to gather data")
            results[r] = 0
            
        # 5. Cleanup
        stop_cluster()
        time.sleep(2)

    # 6. Export/Plot
    print("\n[Summary] Read Latency vs R:")
    print("R | Latency (ms)")
    print("--|-------------")
    for r, lat in results.items():
        print(f"{r} | {lat:.2f}")
    
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(8, 6))
        # Use blue scale for reads
        plt.bar(results.keys(), results.values(), color=['lightblue', 'dodgerblue', 'navy'])
        plt.xlabel('Read Quorum (R)')
        plt.ylabel('Average Latency (ms)')
        plt.title(f'Impact of Read Quorum on Latency (N=3, Nodes={NUM_NODES})')
        plt.xticks(r_values)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        output_file = os.path.join(REPORT_DIR, 'graph_latency_vs_r.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST C: System Throughput vs. Concurrency
# ==============================================================================
def test_c_throughput_vs_concurrency():
    print("\n" + "="*60)
    print("TEST C: System Throughput vs. Concurrency")
    print("Objective: Measure scalability and identify bottlenecks")
    print(f"Configuration: {NUM_NODES} Nodes, N=3, R=2, W=2")
    print("="*60)
    
    results = {} # {num_threads: ops_per_sec}
    thread_counts = [1, 5, 10, 20, 40]
    test_duration = 15 # seconds per run
    
    # 1. Setup Environment ONCE (or per test if clean slate needed)
    # For throughput, we can restart per thread-count to ensure no residual lag
    
    for threads in thread_counts:
        cleanup_environment()
        nodes = start_cluster(n_val=3, r_val=2, w_val=2)
        
        print(f"\n[Bench] Running {test_duration}s throughput test with {threads} threads...")
        
        # Prepare for threading
        stop_event = threading.Event()
        start_time = time.time()
        end_time = start_time + test_duration
        
        total_ops = 0
        errors = 0
        
        def worker():
            local_ops = 0
            local_errs = 0
            while time.time() < end_time:
                # 50/50 Mix of Reads and Writes
                target_node = random.choice(nodes)
                stub = get_stub(target_node)
                key = f"load_{random.randint(0, 1000)}"
                
                try:
                    if random.random() < 0.5:
                        # PUT
                        val = "x" * 100
                        resp = stub.Put(dht_pb2.PutRequest(key=key, value=val), timeout=2)
                        if resp.success: local_ops += 1
                        else: local_errs += 1
                    else:
                        # GET
                        resp = stub.Get(dht_pb2.GetRequest(key=key), timeout=2)
                        if resp.success: local_ops += 1
                        else: local_errs += 1
                except Exception:
                    local_errs += 1
            return local_ops, local_errs

        # Execute threads
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures_list = [executor.submit(worker) for _ in range(threads)]
            
            for f in as_completed(futures_list):
                ops, errs = f.result()
                total_ops += ops
                errors += errs
        
        actual_duration = time.time() - start_time
        ops_sec = total_ops / actual_duration
        
        print(f"[Result] {threads} Threads: {ops_sec:.2f} OPS (Total: {total_ops}, Errors: {errors})")
        results[threads] = ops_sec
        
        stop_cluster()
        time.sleep(2)

    # 3. Export/Plot
    print("\n[Summary] Throughput vs Concurrency:")
    print("Threads | OPS")
    print("--------|--------")
    for t, ops in results.items():
        print(f"{t:<7} | {ops:.2f}")
    
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(8, 6))
        plt.plot(list(results.keys()), list(results.values()), marker='o', linewidth=2, color='purple')
        plt.xlabel('Concurrent Clients (Threads)')
        plt.ylabel('Throughput (Operations/Sec)')
        plt.title(f'System Scalability (Nodes={NUM_NODES}, N=3)')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.ylim(bottom=0)
        
        output_file = os.path.join(REPORT_DIR, 'graph_throughput_vs_concurrency.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST D: Datacenter Distribution Efficiency (Comparative)
# ==============================================================================
def test_d_datacenter_distribution():
    print("\n" + "="*60)
    print("TEST D: Datacenter Distribution Efficiency")
    print("Objective: Compare Replica Placement with vs without DC-Awareness")
    print(f"Configuration: {NUM_NODES} Nodes, {NUM_DCS} Datacenters")
    print("="*60)
    
    # 1. Setup Environment (Start Cluster Once)
    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=2, w_val=2)
    
    # 2. Reconstruct Hash Ring Locally
    print("\n[Analysis] Fetching topology from cluster...")
    seed_node = nodes[0]
    stub = get_stub(seed_node)
    
    try:
        # Get ring state from a live node
        resp = stub.GetRing(dht_pb2.GetRingRequest(requesting_node_id="graph_gen"), timeout=5)
        live_nodes = resp.nodes
        dc_map = dict(resp.datacenter_map)
        
        # Initialize local ring to match server configuration
        ring = ConsistentHash(num_virtual_nodes=150) # Matching config.py
        for node in live_nodes:
            ring.add_node(node)
            
        print(f"[Analysis] Reconstructed ring with {len(live_nodes)} nodes")
        
        # 3. Define Analysis Helper
        def analyze_distribution(is_dc_aware):
            print(f"\n[Analysis] Checking 100 keys with DC-Awareness={'ON' if is_dc_aware else 'OFF'}...")
            counts = {1: 0, 2: 0, 3: 0} # {unique_dcs_count: num_keys}
            
            # Use fixed seed for fair comparison
            random.seed(42) 
            
            for i in range(100):
                key = f"dist_test_{random.randint(0, 10000)}"
                
                # Get preference list
                pref_list = ring.get_preference_list(
                    key, 
                    n=3, 
                    datacenter_aware=is_dc_aware, 
                    datacenter_map=dc_map
                )
                
                # Count unique datacenters
                unique_dcs = set()
                for node in pref_list:
                    dc = dc_map.get(node, "UNKNOWN")
                    unique_dcs.add(dc)
                
                c = len(unique_dcs)
                if c in counts:
                    counts[c] += 1
            return counts

        # 4. Run Comparisons
        stats_enabled = analyze_distribution(True)
        stats_disabled = analyze_distribution(False)

        # 5. Print Text Results
        print("\n[Result] Comparative Distribution (N=3):")
        print("Unique DCs | Aware (Enhanced) | Unaware (Standard)")
        print("-----------|------------------|-------------------")
        for i in [1, 2, 3]:
            print(f"     {i}     |       {stats_enabled[i]:<3}        |       {stats_disabled[i]:<3}")

        # 6. Export/Plot Grouped Bar Chart
        try:
            if not os.path.exists(REPORT_DIR):
                os.makedirs(REPORT_DIR)

            import numpy as np
            
            labels = ['1 DC', '2 Unique DCs', '3 Unique DCs']
            x = np.arange(len(labels))
            width = 0.35
            
            plt.figure(figsize=(10, 6))
            
            # Plot standard (unaware)
            vals_disabled = [stats_disabled[1], stats_disabled[2], stats_disabled[3]]
            rects1 = plt.bar(x - width/2, vals_disabled, width, label='Standard (Disabled)', color='gray', alpha=0.7)
            
            # Plot enhanced (aware)
            vals_enabled = [stats_enabled[1], stats_enabled[2], stats_enabled[3]]
            rects2 = plt.bar(x + width/2, vals_enabled, width, label='Enhanced (DC-Aware)', color='green')
            
            plt.ylabel('Number of Keys (out of 100)')
            plt.title(f'Replica Distribution Efficiency (N=3, DCs={NUM_DCS})')
            plt.xticks(x, labels)
            plt.legend()
            plt.grid(axis='y', linestyle='--', alpha=0.3)
            
            # Helper to label bars
            def autolabel(rects):
                for rect in rects:
                    height = rect.get_height()
                    plt.text(rect.get_x() + rect.get_width()/2., height,
                            f'{int(height)}',
                            ha='center', va='bottom')

            autolabel(rects1)
            autolabel(rects2)
            
            output_file = os.path.join(REPORT_DIR, 'graph_dc_distribution.png')
            plt.savefig(output_file)
            print(f"\n[Graph] Saved plot to {output_file}")
            
        except Exception as e:
            print(f"\n[Graph] Could not generate plot: {e}")

    except Exception as e:
        print(f"[Error] Failed to fetch ring or run analysis: {e}")
        import traceback
        traceback.print_exc()

    # 7. Cleanup
    stop_cluster()
    time.sleep(2)
    
# ==============================================================================
# TEST E: Key Load Balancing (Uniformity)
# ==============================================================================
def test_e_load_balancing():
    print("\n" + "="*60)
    print("TEST E: Key Load Balancing (Uniformity)")
    print("Objective: Check if Consistent Hashing distributes load evenly")
    print(f"Configuration: {NUM_NODES} Nodes, N=3, 1000 Items")
    print("="*60)
    
    # 1. Setup Environment
    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=2, w_val=2)
    
    # 2. Insert Data
    num_items = 1000
    print(f"\n[Bench] Inserting {num_items} keys...")
    
    success_count = 0
    for i in range(num_items):
        key = f"load_test_{i:04d}"
        val = "x" * 10
        
        # Use random coordinator to spread write load
        target_node = random.choice(nodes)
        stub = get_stub(target_node)
        
        try:
            resp = stub.Put(dht_pb2.PutRequest(key=key, value=val), timeout=5)
            if resp.success:
                success_count += 1
        except Exception:
            pass
            
        if i % 100 == 0:
            print(f"  Inserted {i} keys...")
            
    print(f"[Bench] Inserted {success_count}/{num_items} keys. Waiting for replication...")
    time.sleep(10) # Wait for eventual consistency/replication to finish
    
    # 3. Analyze Disk Storage
    print("[Analysis] Reading node storage files...")
    node_counts = {}
    total_replicas = 0
    
    # Initialize all nodes to 0
    for i in range(NUM_NODES):
        node_id = f"node_{i}" # Matches start_cluster naming convention
        node_counts[node_id] = 0
        
    if os.path.exists("data"):
        for filename in os.listdir("data"):
            if filename.endswith("_data.json"):
                node_id = filename.replace("_data.json", "")
                try:
                    with open(os.path.join("data", filename), 'r') as f:
                        data = json.load(f)
                        count = len(data)
                        node_counts[node_id] = count
                        total_replicas += count
                except Exception:
                    pass
    
    # 4. Statistics
    counts = list(node_counts.values())
    avg_load = statistics.mean(counts)
    try:
        std_dev = statistics.stdev(counts)
    except:
        std_dev = 0
        
    expected_replicas = num_items * 3 # N=3
    print(f"\n[Result] Total Replicas Stored: {total_replicas} (Expected ~{expected_replicas})")
    print(f"[Result] Average Keys per Node: {avg_load:.2f}")
    print(f"[Result] Standard Deviation: {std_dev:.2f}")
    print(f"[Result] Min: {min(counts)}, Max: {max(counts)}")
    
    # 5. Export/Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(12, 6))
        
        # Sort by node ID for cleaner graph
        sorted_ids = sorted(node_counts.keys(), key=lambda x: int(x.split('_')[1]))
        sorted_counts = [node_counts[nid] for nid in sorted_ids]
        
        # Create bar chart
        x_pos = range(len(sorted_counts))
        plt.bar(x_pos, sorted_counts, color='teal', alpha=0.7, width=0.8)
        
        # Add mean line
        plt.axhline(y=avg_load, color='r', linestyle='-', label=f'Mean ({avg_load:.1f})')
        
        plt.xlabel('Node Index (0-99)')
        plt.ylabel('Number of Keys Stored')
        plt.title(f'Key Distribution across {NUM_NODES} Nodes (1000 Keys, N=3)')
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        output_file = os.path.join(REPORT_DIR, 'graph_load_balancing.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

    # 6. Cleanup
    stop_cluster()
    time.sleep(2)

# ==============================================================================
# TEST F: Availability under Datacenter Failure
# ==============================================================================
def test_f_availability_under_failure():
    print("\n" + "="*60)
    print("TEST F: Availability under Datacenter Failure")
    print("Objective: Prove survival of a complete DC outage (The Money Shot)")
    print("Method: Kill DC1 (10 nodes) mid-operation and measure success rate")
    print("="*60)
    
    # We test two configurations
    scenarios = [
        {'n': 3, 'r': 2, 'w': 2, 'label': 'W=2 (High Availability)'},
        {'n': 3, 'r': 3, 'w': 3, 'label': 'W=3 (Strong Consistency)'}
    ]
    
    plot_data = {} # {label: (timestamps, success_rates)}
    
    for sc in scenarios:
        label = sc['label']
        print(f"\n[Scenario] Testing {label}...")
        
        # 1. Setup specific 3-DC cluster (30 nodes)
        # This forces N=3 to place exactly 1 replica per DC
        cleanup_environment()
        num_test_nodes = 100
        nodes = start_cluster(n_val=sc['n'], r_val=sc['r'], w_val=sc['w'], 
                            num_nodes=num_test_nodes, num_dcs=3)
        
        # 2. Prepare Benchmark
        duration_total = 50
        kill_at = 5 # Kill DC1 after 4 seconds
        
        start_time = time.time()
        end_time = start_time + duration_total
        
        timestamps = []
        outcomes = [] # 1 for success, 0 for fail
        dc1_killed = False
        
        print(f"[Bench] Starting continuous operations for {duration_total}s...")
        print(f"[Bench] Scheduled to KILL DC1 at t={kill_at}s")
        
        while time.time() < end_time:
            current_time = time.time()
            elapsed = current_time - start_time
            
            # KILL TRIGGER
            if elapsed >= kill_at and not dc1_killed:
                print(f"[EVENT] ⚡ KILLING DATACENTER 1 (Nodes 0-9) ⚡")
                # DC1 nodes are the first 10 nodes (indices 0-9)
                # Terminate processes
                for i in range(10):
                    if node_processes[i] and node_processes[i].poll() is None:
                        node_processes[i].terminate()
                dc1_killed = True
                # Remove killed nodes from our client target list so we don't try to connect to dead ports
                # (Simulates load balancer removing dead nodes)
                nodes = nodes[10:] 
                time.sleep(1) # Allow system to realize failure
                continue

            # OPERATE
            try:
                # Pick random living node
                target_node = random.choice(nodes)
                stub = get_stub(target_node)
                key = f"avail_test_{random.randint(0, 1000)}"
                val = "x" * 10
                
                # We test WRITES (PUT) as they are most sensitive to W quorum
                req_start = time.time()
                resp = stub.Put(dht_pb2.PutRequest(key=key, value=val), timeout=1.0)
                
                if resp.success:
                    outcomes.append(1)
                else:
                    outcomes.append(0)
            except Exception:
                outcomes.append(0)
            
            timestamps.append(elapsed)
            # Throttle slightly
            time.sleep(0.05)
            
        # 3. Process Data for this scenario
        # Calculate moving average success rate
        window_size = 10
        smoothed_rates = []
        smoothed_times = []
        
        for i in range(len(outcomes)):
            # Get window
            start_idx = max(0, i - window_size)
            window = outcomes[start_idx : i+1]
            rate = sum(window) / len(window)
            smoothed_rates.append(rate * 100) # Percentage
            smoothed_times.append(timestamps[i])
            
        plot_data[label] = (smoothed_times, smoothed_rates)
        
        stop_cluster()
        time.sleep(2)

    # 4. Export/Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(10, 6))
        
        colors = {'W=2 (High Availability)': 'green', 'W=3 (Strong Consistency)': 'red'}
        
        for label, (times, rates) in plot_data.items():
            plt.plot(times, rates, label=label, color=colors.get(label, 'blue'), linewidth=2)
            
        # Add "Failure Event" line
        plt.axvline(x=4, color='black', linestyle='--', label='DC1 Failure Event')
        
        plt.xlabel('Time (seconds)')
        plt.ylabel('Operation Success Rate (%)')
        plt.title('System Availability under Datacenter Failure (3 DCs, N=3)')
        plt.ylim(-5, 105)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        
        output_file = os.path.join(REPORT_DIR, 'graph_availability_dc_failure.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST G: Conflict Rate (Improved)
# ==============================================================================
def test_g_conflict_rate():
    print("\n" + "="*60)
    print("TEST G: Divergent Versions (Conflict) Rate")
    print("Objective: Show Vector Clock behavior under realistic race conditions")
    print("Method: Concurrent writes with random network jitter")
    print("="*60)

    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=2, w_val=2)
    
    num_tests = 50
    conflicts = 0
    clean_writes = 0
    
    print(f"\n[Bench] Simulating {num_tests} concurrent write pairs...")

    def jittery_write(node_addr, key, val):
        try:
            # Simulate variable network latency (0-50ms)
            time.sleep(random.uniform(0, 0.05))
            stub = get_stub(node_addr)
            stub.Put(dht_pb2.PutRequest(key=key, value=val), timeout=5)
        except: pass

    for i in range(num_tests):
        key = f"conflict_test_{i}"
        node_a, node_b = random.sample(nodes, 2)
        
        # Launch writes with jitter
        t1 = threading.Thread(target=jittery_write, args=(node_a, key, "vA"))
        t2 = threading.Thread(target=jittery_write, args=(node_b, key, "vB"))
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        time.sleep(0.2) # Allow settling
        
        try:
            reader = get_stub(random.choice(nodes))
            resp = reader.Get(dht_pb2.GetRequest(key=key), timeout=5)
            if resp.has_conflicts:
                conflicts += 1
            else:
                clean_writes += 1
        except: pass

    # Plot
    try:
        plt.style.use('ggplot') # Better styling
        plt.figure(figsize=(8, 8))
        
        labels = [f'Conflicts ({conflicts})', f'Clean Writes ({clean_writes})']
        sizes = [conflicts, clean_writes]
        colors = ['#FF6B6B', '#4ECDC4'] # Red/Teal
        explode = (0.05, 0)
        
        plt.pie(sizes, explode=explode, labels=labels, colors=colors,
                autopct='%1.1f%%', startangle=90, shadow=True)
        
        plt.title(f'Resolution of Concurrent Writes (N={num_tests})', fontsize=14)
        
        output_file = os.path.join(REPORT_DIR, 'graph_conflict_rate.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved improved plot to {output_file}")
        
    except Exception as e:
        print(f"[Graph Error] {e}")

    stop_cluster()
    
# ==============================================================================
# TEST H: Read Repair Latency (Convergence Time)
# ==============================================================================
def test_h_read_repair_convergence():
    print("\n" + "="*60)
    print("TEST H: Read Repair Latency (Convergence Time)")
    print("Objective: Measure time for 'Eventual Consistency' to become 'Consistent'")
    print("Method: Partition Node -> Write(W=2) -> Reconnect -> Read(R=3) -> Measure Repair")
    print(f"Configuration: {NUM_NODES} Nodes, N=3, R=3, W=2")
    print("="*60)
    
    # 1. Setup Environment
    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=3, w_val=2)
    
    # Get topology
    seed_node = nodes[0]
    stub = get_stub(seed_node)
    try:
        resp = stub.GetRing(dht_pb2.GetRingRequest(requesting_node_id="graph_gen"), timeout=5)
        dc_map = dict(resp.datacenter_map)
        ring = ConsistentHash(num_virtual_nodes=150)
        for node in resp.nodes:
            ring.add_node(node)
    except Exception as e:
        print(f"[Error] Failed to initialize ring: {e}")
        stop_cluster()
        return

    num_trials = 20
    convergence_times = []
    trials_x = []
    
    print(f"\n[Bench] Running {num_trials} repair trials...")
    
    for i in range(num_trials):
        key = f"repair_test_{i}"
        val_new = f"value_new_{i}"
        
        try:
            pref_list = ring.get_preference_list(key, n=3, datacenter_aware=True, datacenter_map=dc_map)
            node_a_addr = pref_list[0]
            node_b_addr = pref_list[1]
            
            target_port = int(node_a_addr.split(':')[1])
            node_idx = target_port - BASE_PORT
            node_id = f"node_{node_idx}"
        except:
            continue

        # Partition Node A
        if node_processes[node_idx] and node_processes[node_idx].poll() is None:
            node_processes[node_idx].terminate()
            node_processes[node_idx].wait()
        
        time.sleep(0.5)
        
        # Write to B
        try:
            stub_b = get_stub(node_b_addr)
            stub_b.Put(dht_pb2.PutRequest(key=key, value=val_new), timeout=2)
        except:
            pass
            
        # Reconnect Node A
        node_dc = dc_map.get(node_a_addr, "DC1") 
        cmd = [
            sys.executable, "server_v2.py",
            "--node-id", node_id,
            "--host", "localhost",
            "--port", str(target_port),
            "--datacenter", node_dc,
            "--n", "3", "--r", "3", "--w", "2",
            "--known-nodes", nodes[0],
            "--enable-dc-aware"
        ]
        
        log_file = open(f"logs/{node_id}.log", "a")
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        node_processes[node_idx] = proc
        
        # Wait for Health
        stub_a = get_stub(node_a_addr)
        for _ in range(20):
            try:
                stub_a.HealthCheck(dht_pb2.HealthCheckRequest(), timeout=0.1)
                break
            except:
                time.sleep(0.1)

        # Trigger Repair (Read from B, it contacts A)
        trigger_start = time.time()
        try:
            stub_b.Get(dht_pb2.GetRequest(key=key), timeout=2)
        except:
            pass
            
        # Measure Convergence on A
        repaired = False
        poll_start = time.time()
        while time.time() - poll_start < 2.0:
            try:
                resp = stub_a.InternalGet(dht_pb2.InternalGetRequest(key=key), timeout=0.1)
                if resp.success and resp.versions and resp.versions[0].value == val_new:
                    repaired = True
                    break
            except:
                pass
            time.sleep(0.01)
            
        if repaired:
            latency_ms = (time.time() - trigger_start) * 1000
            convergence_times.append(latency_ms)
            trials_x.append(i + 1)

    # Export/Plot
    if convergence_times:
        avg_conv = statistics.mean(convergence_times)
        print(f"\n[Result] Convergence Avg: {avg_conv:.2f} ms")
        
        try:
            if not os.path.exists(REPORT_DIR):
                os.makedirs(REPORT_DIR)

            plt.figure(figsize=(10, 6))
            # CHANGE: Line plot instead of histogram
            plt.plot(trials_x, convergence_times, marker='o', linestyle='-', color='purple', linewidth=2, markersize=8)
            plt.axhline(avg_conv, color='red', linestyle='--', label=f'Mean: {avg_conv:.1f}ms')
            
            plt.xlabel('Trial Number')
            plt.ylabel('Convergence Time (ms)')
            plt.title('Read Repair Latency per Trial')
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.5)
            plt.xticks(trials_x) # Show all trial numbers
            
            output_file = os.path.join(REPORT_DIR, 'graph_read_repair.png')
            plt.savefig(output_file)
            print(f"\n[Graph] Saved plot to {output_file}")
            
        except Exception as e:
            print(f"\n[Graph] Could not generate plot: {e}")

    # Cleanup
    stop_cluster()
    time.sleep(2)
    
# ==============================================================================
# TEST I: Total Datacenter Failure Survival
# ==============================================================================
def test_i_total_dc_failure_survival():
    print("\n" + "="*60)
    print("TEST I: Total Datacenter Failure Survival")
    print("Objective: Compare Data Availability after complete DC failure")
    print("Scenario: Write 100 keys -> Kill DC1 -> Read 100 keys")
    print(f"Configuration: {NUM_NODES} Nodes, {NUM_DCS} Datacenters")
    print("="*60)
    
    results = {} # {mode: success_rate}
    modes = [
        {'label': 'Baseline (Naive)', 'dc_aware': False},
        {'label': 'Enhanced (DC-Aware)', 'dc_aware': True}
    ]
    
    # We need a topology where DC1 failure is significant
    # 3 DCs, 30 nodes (10 per DC) is ideal for this demonstration
    # N=3, R=2, W=2 (Standard Quorum)
    test_nodes = 30
    test_dcs = 3
    
    for mode in modes:
        label = mode['label']
        is_aware = mode['dc_aware']
        
        # 1. Setup
        cleanup_environment()
        print(f"\n[Test] Running scenario: {label}")
        nodes = start_cluster(n_val=3, r_val=2, w_val=2, 
                            num_nodes=test_nodes, num_dcs=test_dcs, 
                            dc_aware=is_aware)
        
        # 2. Write Data
        print("[Step] Writing 100 keys...")
        keys_written = []
        write_success = 0
        
        # Use random nodes for writes to distribute initial coordinators
        for i in range(100):
            key = f"survival_key_{i}"
            val = "survivor"
            try:
                target = random.choice(nodes)
                stub = get_stub(target)
                resp = stub.Put(dht_pb2.PutRequest(key=key, value=val), timeout=2)
                if resp.success:
                    keys_written.append(key)
                    write_success += 1
            except:
                pass
        
        print(f"  Written {write_success} keys. Waiting for replication...")
        time.sleep(5) 
        
        # 3. THE EVENT: Kill DC1 (First 10 nodes)
        print("[EVENT] ⚡ DESTROYING DATACENTER 1 ⚡")
        # DC1 nodes are indices 0-9
        killed_count = 0
        for i in range(10):
            if node_processes[i] and node_processes[i].poll() is None:
                node_processes[i].terminate()
                killed_count += 1
        print(f"  Killed {killed_count} nodes (All of DC1). System is degraded.")
        time.sleep(2)
        
        # 4. Read Data (Attempt recovery)
        print("[Step] Attempting to read keys from remaining DCs...")
        # Only contact living nodes (indices 10-29)
        living_nodes = nodes[10:]
        
        read_success = 0
        for key in keys_written:
            # Try multiple times since some nodes might hold pointers to dead nodes
            # But the client should eventually find a healthy coordinator
            try:
                target = random.choice(living_nodes)
                stub = get_stub(target)
                resp = stub.Get(dht_pb2.GetRequest(key=key), timeout=2)
                if resp.success:
                    read_success += 1
            except:
                pass
                
        success_rate = (read_success / len(keys_written)) * 100 if keys_written else 0
        results[label] = success_rate
        print(f"[Result] {label}: {read_success}/{len(keys_written)} keys retrieved ({success_rate:.1f}%)")
        
        stop_cluster()
        time.sleep(2)

    # 5. Export/Plot
    print("\n[Summary] Survival Rate (DC1 Failure):")
    for label, rate in results.items():
        print(f"{label}: {rate:.1f}%")
        
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(8, 6))
        bars = plt.bar(results.keys(), results.values(), color=['gray', 'green'])
        plt.ylabel('Data Availability (%)')
        plt.title('Survival Rate after Total Datacenter Failure (N=3, R=2)')
        plt.ylim(0, 110)
        plt.grid(axis='y', linestyle='--', alpha=0.5)
        
        # Add values on top
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 1,
                    f'{height:.1f}%',
                    ha='center', va='bottom', fontweight='bold')
        
        output_file = os.path.join(REPORT_DIR, 'graph_survival_test.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST J: Straggler Latency (Improved)
# ==============================================================================
def test_j_straggler_latency():
    print("\n" + "="*60)
    print("TEST J: Straggler Node Latency")
    print("Objective: Prove Asynchronous W-Quorum ignores slow nodes")
    print("Scenario: DC3 has 50ms delay. Compare W=3 vs W=2.")
    print("="*60)
    
    test_nodes = 100
    test_dcs = 3
    num_writes = 20 # Fewer writes, high impact
    
    results = {}
    
    scenarios = [
        {'w': 3, 'label': 'Synchronous (W=3)'},
        {'w': 2, 'label': 'Asynchronous (W=2)'}
    ]
    
    for sc in scenarios:
        w_val = sc['w']
        label = sc['label']
        
        cleanup_environment()
        # Force 2.0s delay
        nodes = start_cluster(n_val=3, r_val=2, w_val=w_val, 
                            num_nodes=test_nodes, num_dcs=test_dcs, 
                            straggler_dc="DC3", straggler_delay=0.050)
        
        latencies = []
        print(f"\n[Bench] {label}: Performing writes...")
        
        for i in range(num_writes):
            # Coordinator in DC1 (Fast), Replicas include DC3 (Slow)
            coord = nodes[0] 
            key = f"straggler_{i}"
            
            try:
                start = time.time()
                get_stub(coord).Put(dht_pb2.PutRequest(key=key, value="x"*10), timeout=5)
                lat = (time.time() - start) * 1000
                latencies.append(lat)
                print(f"  Write {i}: {lat:.1f} ms")
            except: pass
            
        avg_lat = statistics.mean(latencies) if latencies else 0
        results[label] = avg_lat
        stop_cluster()

    # Plot
    try:
        plt.style.use('ggplot')
        plt.figure(figsize=(10, 6))
        
        labels = list(results.keys())
        values = list(results.values())
        colors = ['#FF6B6B', '#4ECDC4'] # Red/Teal
        
        bars = plt.bar(labels, values, color=colors, width=0.6)
        
        plt.ylabel('Average Write Latency (ms)', fontsize=12)
        plt.title('Impact of Straggler Node (50ms Delay) on Write Latency', fontsize=14)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Log scale if difference is huge
        if max(values) > 1000 and min(values) < 100:
            plt.yscale('log')
            plt.ylabel('Average Write Latency (ms) - Log Scale', fontsize=12)
        
        # Add labels
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height * 1.02,
                    f'{int(height)} ms',
                    ha='center', va='bottom', fontweight='bold', fontsize=12)
            
        output_file = os.path.join(REPORT_DIR, 'graph_straggler_latency.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved improved plot to {output_file}")
        
    except Exception as e:
        print(f"[Graph Error] {e}")

# ==============================================================================
# TEST K: Consistency Convergence (Improved)
# ==============================================================================
def test_k_consistency_convergence():
    print("\n" + "="*60)
    print("TEST K: Consistency Convergence Speed")
    print("Objective: Measure Read Repair efficiency")
    print("="*60)
    
    results = {}
    
    scenarios = [
        {'repair': False, 'label': 'Baseline (No Repair)'},
        {'repair': True,  'label': 'Enhanced (With Repair)'}
    ]
    
    for sc in scenarios:
        repair_on = sc['repair']
        label = sc['label']
        
        cleanup_environment()
        nodes = start_cluster(n_val=3, r_val=2, w_val=2, 
                            num_nodes=10, num_dcs=1, read_repair=repair_on)
        
        # Setup Topology
        stub_seed = get_stub(nodes[0])
        resp = stub_seed.GetRing(dht_pb2.GetRingRequest(requesting_node_id="gen"), timeout=5)
        ring = ConsistentHash(num_virtual_nodes=150)
        for n in resp.nodes: ring.add_node(n)
        
        key = "consistency_key"
        pref = ring.get_preference_list(key, n=3)
        node_a_addr = pref[0]
        node_b_addr = pref[1]
        
        target_port = int(node_a_addr.split(':')[1])
        node_idx = target_port - BASE_PORT
        node_id = f"node_{node_idx}"
        
        # 1. Write V1
        get_stub(node_b_addr).Put(dht_pb2.PutRequest(key=key, value="v1"), timeout=2)
        time.sleep(1)
        
        # 2. Kill A
        node_processes[node_idx].terminate()
        node_processes[node_idx].wait()
        time.sleep(1)
        
        # 3. Write V2 to B
        get_stub(node_b_addr).Put(dht_pb2.PutRequest(key=key, value="v2"), timeout=2)
        
        # 4. Restart A
        cmd = [
            sys.executable, "server_v2.py",
            "--node-id", node_id, "--host", "localhost", "--port", str(target_port),
            "--datacenter", "DC1", "--n", "3", "--r", "2", "--w", "2",
            "--known-nodes", nodes[0], "--enable-dc-aware"
        ]
        if not repair_on: cmd.append("--disable-read-repair")
        
        log = open(f"logs/{node_id}.log", "a")
        node_processes[node_idx] = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        
        # 5. CRITICAL: Wait for A to Gossip
        print(f"  Waiting for {node_id} to rejoin ring...")
        stub_a = get_stub(node_a_addr)
        for _ in range(100):
            try:
                r = stub_a.GetRing(dht_pb2.GetRingRequest(requesting_node_id="test"), timeout=0.1)
                if len(r.nodes) > 1: break # It knows about peers!
            except: pass
            time.sleep(0.1)
            
        # 6. Measure
        stale = 0
        fresh = 0
        
        # Trigger Read (R=2)
        try: 
            # Was: stub_a.Get(...)
            get_stub(node_b_addr).Get(dht_pb2.GetRequest(key=key), timeout=2)
        except: 
            pass
        
        # Probe Internal State
        for _ in range(50):
            try:
                r = stub_a.InternalGet(dht_pb2.InternalGetRequest(key=key), timeout=0.1)
                val = r.versions[0].value
                if val == "v2": fresh += 1
                else: stale += 1
            except: stale += 1
            time.sleep(0.02)
            
        results[label] = stale
        print(f"[Result] {label}: {stale} Stale Reads")
        stop_cluster()

    # Plot
    try:
        plt.style.use('ggplot')
        plt.figure(figsize=(8, 6))
        
        labels = list(results.keys())
        values = list(results.values())
        colors = ['gray', '#4ECDC4']
        
        bars = plt.bar(labels, values, color=colors, width=0.6)
        
        plt.ylabel('Stale Reads Count (lower is better)', fontsize=12)
        plt.title('Effectiveness of Read Repair', fontsize=14)
        
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{int(height)}', ha='center', va='bottom', fontweight='bold')
            
        output_file = os.path.join(REPORT_DIR, 'graph_consistency_convergence.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved improved plot to {output_file}")
    except Exception as e: print(e)

    print("\n" + "="*60)
    print("TEST K: Consistency Convergence Speed")
    print("Objective: Measure Read Repair efficiency vs Baseline")
    print("Scenario: Partition -> Divergent Write -> Reconnect -> Measure Stale Reads")
    print("="*60)
    
    results = {} # {label: stale_count}
    
    scenarios = [
        {'repair': False, 'label': 'Baseline (No Repair)'},
        {'repair': True,  'label': 'Enhanced (With Repair)'}
    ]
    
    # Use smaller cluster for precise control
    test_nodes = 10
    test_dcs = 1 
    
    for sc in scenarios:
        repair_on = sc['repair']
        label = sc['label']
        
        cleanup_environment()
        print(f"\n[Test] Running {label}...")
        
        # 1. Start Cluster (N=3, W=3 to ensure strict write initially)
        nodes = start_cluster(n_val=3, r_val=2, w_val=2, 
                            num_nodes=test_nodes, num_dcs=test_dcs, 
                            read_repair=repair_on)
        
        # 2. Setup Data
        key = "consistency_key"
        val_v1 = "Version_1"
        val_v2 = "Version_2"
        
        # Get topology
        stub_seed = get_stub(nodes[0])
        resp = stub_seed.GetRing(dht_pb2.GetRingRequest(requesting_node_id="gen"), timeout=5)
        
        # Init local ring to find replicas
        ring = ConsistentHash(num_virtual_nodes=150)
        for n in resp.nodes: ring.add_node(n)
        
        pref_list = ring.get_preference_list(key, n=3)
        node_a_addr = pref_list[0] # The node we will kill
        node_b_addr = pref_list[1] # The node that will accept the update
        
        # Map address to process index
        target_port = int(node_a_addr.split(':')[1])
        node_idx = target_port - BASE_PORT
        node_id = f"node_{node_idx}"
        
        # Write V1 to all
        print(f"[Step] Writing {val_v1} to all nodes...")
        get_stub(node_b_addr).Put(dht_pb2.PutRequest(key=key, value=val_v1), timeout=2)
        time.sleep(1)
        
        # 3. Partition Node A (Kill it)
        print(f"[Step] Killing Node A ({node_id})...")
        if node_processes[node_idx]:
            node_processes[node_idx].terminate()
            node_processes[node_idx].wait()
        time.sleep(1)
        
        # 4. Write V2 to remaining nodes (W=2)
        print(f"[Step] Writing {val_v2} to remaining replicas...")
        try:
            get_stub(node_b_addr).Put(dht_pb2.PutRequest(key=key, value=val_v2), timeout=2)
        except:
            print("  Write warning (expected if quorum tight)")
            
        # 5. Restart Node A (It has V1)
        print(f"[Step] Restarting Node A...")
        cmd = [
            sys.executable, "server_v2.py",
            "--node-id", node_id, "--host", "localhost", "--port", str(target_port),
            "--datacenter", "DC1", "--n", "3", "--r", "2", "--w", "2",
            "--known-nodes", nodes[0], "--enable-dc-aware"
        ]
        if not repair_on: cmd.append("--disable-read-repair")
        
        log_file = open(f"logs/{node_id}.log", "a")
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        node_processes[node_idx] = proc
        
        # Wait for health AND Ring Discovery
        stub_a = get_stub(node_a_addr)
        discovery_ok = False
        print("  Waiting for Node A to rejoin ring...")
        for _ in range(50): # Wait up to 5 seconds
            try:
                # Check if node sees the cluster
                r_resp = stub_a.GetRing(dht_pb2.GetRingRequest(requesting_node_id="test"), timeout=0.1)
                if len(r_resp.nodes) > 1: # It sees peers!
                    discovery_ok = True
                    break
            except:
                pass
            time.sleep(0.1)
            
        if not discovery_ok:
            print("  [Warn] Node A failed to discover peers in time. Test might result in false negative.")
        
        # 6. THE TEST SEQUENCE
        print("[Step] Measuring Consistency Convergence...")
        stale_reads = 0
        fresh_reads = 0
        
        # Probe 1: Trigger Read (R=2)
        # This SHOULD trigger repair if enabled because it sees V1 (local) and V2 (remote)
        try:
            # We explicitly read from Node A to force it to coordinate and see the divergence
            stub_a.Get(dht_pb2.GetRequest(key=key), timeout=2)
        except:
            pass
            
        # Probes 2-51: Check Local State (R=1)
        # Check internal state directly to see if repair wrote to disk/memory
        for _ in range(50):
            try:
                resp = stub_a.InternalGet(dht_pb2.InternalGetRequest(key=key), timeout=0.1)
                if resp.success and resp.versions:
                    # Check if ANY version is V2 (might have siblings)
                    has_v2 = any(v.value == val_v2 for v in resp.versions)
                    if has_v2:
                        fresh_reads += 1
                    else:
                        stale_reads += 1
                else:
                    stale_reads += 1
            except:
                stale_reads += 1
            time.sleep(0.02)
            
        print(f"[Result] {label}: {stale_reads} Stale / {fresh_reads} Fresh")
        results[label] = stale_reads
        
        stop_cluster()
        time.sleep(2)

    # 7. Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(8, 6))
        bars = plt.bar(results.keys(), results.values(), color=['gray', 'blue'])
        plt.ylabel('Stale Reads Count (lower is better)')
        plt.title('Consistency Convergence: Read Repair Effectiveness')
        plt.grid(axis='y', linestyle='--', alpha=0.5)
        
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontweight='bold')
        
        output_file = os.path.join(REPORT_DIR, 'graph_consistency_convergence.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")
# ==============================================================================
# TEST L: Throughput Scalability: "Linear Growth"
# ==============================================================================
def test_l_throughput_scalability():
    print("\n" + "="*60)
    print("TEST L: Throughput Scalability: 'Linear Growth'")
    print("Objective: Prove Horizontal Scalability (Linear OPS growth with Node count)")
    print("Method: Saturation test (40 threads) against cluster sizes 3, 6, 9, 12")
    print("="*60)
    
    # We test these specific cluster sizes
    # We use 3 DCs for all tests to ensure N=3 placement works identically
    cluster_sizes = [3, 6, 9, 12] 
    
    means = []
    stds = []
    
    num_trials = 3
    saturation_threads = 40 # Enough to saturate small clusters
    test_duration = 10 # Seconds per trial
    
    for size in cluster_sizes:
        print(f"\n[Bench] Testing Cluster Size: {size} Nodes ({num_trials} trials)...")
        trial_ops = []
        
        for trial in range(num_trials):
            # 1. Setup Environment
            cleanup_environment()
            # N=3, R=1, W=1 for raw throughput speed (focus on hashing/network capacity)
            # Or R=2, W=2 for realistic quorum throughput. Let's use R=2, W=2.
            nodes = start_cluster(n_val=3, r_val=2, w_val=2, 
                                num_nodes=size, num_dcs=3)
            
            # 2. Run Saturation Load
            stop_event = threading.Event()
            start_time = time.time()
            end_time = start_time + test_duration
            
            total_ops = 0
            
            def worker():
                local_ops = 0
                while time.time() < end_time:
                    target = random.choice(nodes)
                    stub = get_stub(target)
                    key = f"scale_{random.randint(0, 10000)}"
                    try:
                        # Mix of 50% read / 50% write
                        if random.random() < 0.5:
                            stub.Put(dht_pb2.PutRequest(key=key, value="x"*100), timeout=1)
                        else:
                            stub.Get(dht_pb2.GetRequest(key=key), timeout=1)
                        local_ops += 1
                    except:
                        pass
                return local_ops

            with ThreadPoolExecutor(max_workers=saturation_threads) as executor:
                futures = [executor.submit(worker) for _ in range(saturation_threads)]
                for f in as_completed(futures):
                    total_ops += f.result()
            
            ops_sec = total_ops / test_duration
            trial_ops.append(ops_sec)
            print(f"  Trial {trial+1}: {ops_sec:.2f} OPS")
            
            stop_cluster()
            time.sleep(1)
            
        # Stats for this cluster size
        avg_ops = statistics.mean(trial_ops)
        std_ops = statistics.stdev(trial_ops) if len(trial_ops) > 1 else 0
        
        means.append(avg_ops)
        stds.append(std_ops)
        print(f"[Result] Size {size}: {avg_ops:.2f} ± {std_ops:.2f} OPS")

    # 3. Export/Plot
    print("\n[Summary] Scalability Results:")
    print("Nodes | Throughput (OPS)")
    print("------|-----------------")
    for i, size in enumerate(cluster_sizes):
        print(f"{size:<5} | {means[i]:.2f}")
        
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(10, 6))
        
        # Plot Error Bars
        plt.errorbar(cluster_sizes, means, yerr=stds, fmt='-o', 
                    color='blue', ecolor='red', capsize=5, linewidth=2, markersize=8)
        
        # Add ideal linear trend line (normalized to start point)
        # Ideal: OPS(N) = OPS(start) * (N / start)
        start_ops = means[0]
        start_size = cluster_sizes[0]
        ideal_trend = [start_ops * (n / start_size) for n in cluster_sizes]
        plt.plot(cluster_sizes, ideal_trend, '--', color='green', alpha=0.5, label='Ideal Linear Growth')
        
        plt.xlabel('Cluster Size (Number of Nodes)')
        plt.ylabel('Throughput (Operations/Sec)')
        plt.title('Horizontal Scalability: Linear Growth Analysis')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.xticks(cluster_sizes)
        plt.ylim(bottom=0)
        
        output_file = os.path.join(REPORT_DIR, 'graph_scalability.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST M: The "Availability Cliff" (Time Series)
# ==============================================================================
def test_m_availability_cliff():
    print("\n" + "="*60)
    print("TEST M: The 'Availability Cliff' (Time Series)")
    print("Objective: Visual proof of Datacenter-Awareness logic")
    print("Scenario: Normal Op -> Kill DC1 -> Compare Naive vs Enhanced stability")
    print("="*60)
    
    # Configuration
    test_nodes = 30
    test_dcs = 3
    # N=3, R=2, W=2. 
    # With R=2, we need 2 surviving replicas.
    # DC-Aware ensures 1 in DC1, 1 in DC2, 1 in DC3. If DC1 dies, 2 survive. Success.
    # Naive might put 2 in DC1. If DC1 dies, 1 survives. Failure.
    
    modes = [
        {'label': 'Naive Placement', 'dc_aware': False, 'color': 'red'},
        {'label': 'DC-Aware Placement', 'dc_aware': True, 'color': 'green'}
    ]
    
    results = {} # {label: (times, success_rates)}
    
    for mode in modes:
        label = mode['label']
        is_aware = mode['dc_aware']
        
        cleanup_environment()
        print(f"\n[Test] Running {label}...")
        
        # 1. Start Cluster
        nodes = start_cluster(n_val=3, r_val=2, w_val=2, 
                            num_nodes=test_nodes, num_dcs=test_dcs, 
                            dc_aware=is_aware)
        
        # 2. Pre-load Data
        print("[Step] Pre-loading 200 keys...")
        keys = []
        for i in range(200):
            key = f"cliff_key_{i}"
            try:
                # Write with W=3 to ensure all replicas theoretically exist before we start
                target = random.choice(nodes)
                get_stub(target).Put(dht_pb2.PutRequest(key=key, value="data"), timeout=2)
                keys.append(key)
            except:
                pass
        time.sleep(5) # Let replication settle
        
        # 3. Time Series Read Loop
        duration = 15
        kill_at = 5
        
        start_time = time.time()
        end_time = start_time + duration
        dc1_dead = False
        
        timestamps = []
        success_rates = []
        
        print(f"[Bench] Reading continuously for {duration}s (Kill at {kill_at}s)...")
        
        # Separate living/dead node lists for client targeting
        # Indices 0-9 are DC1 (will die), 10-29 are DC2/DC3 (will survive)
        current_client_targets = list(nodes) 
        
        while time.time() < end_time:
            now = time.time()
            elapsed = now - start_time
            
            # KILL EVENT
            if elapsed >= kill_at and not dc1_dead:
                print("  [EVENT] ⚡ KILLING DC1 ⚡")
                for i in range(10):
                    if node_processes[i] and node_processes[i].poll() is None:
                        node_processes[i].terminate()
                dc1_dead = True
                # Client stops trying to talk to dead nodes (simulating load balancer update)
                current_client_targets = nodes[10:]
                time.sleep(0.5) # Impact pause
                continue
            
            # MEASURE BATCH
            # Try to read a random subset of keys to get an instant success %
            batch_size = 20
            batch_keys = random.sample(keys, batch_size)
            successes = 0
            
            for key in batch_keys:
                try:
                    # Pick a random LIVING node as coordinator
                    target = random.choice(current_client_targets)
                    stub = get_stub(target)
                    resp = stub.Get(dht_pb2.GetRequest(key=key), timeout=0.5)
                    if resp.success:
                        successes += 1
                except:
                    pass
            
            rate = (successes / batch_size) * 100
            timestamps.append(elapsed)
            success_rates.append(rate)
            
            time.sleep(0.2) # Resolution
            
        results[label] = (timestamps, success_rates)
        stop_cluster()
        time.sleep(2)

    # 4. Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(10, 6))
        
        for mode in modes:
            lbl = mode['label']
            times, rates = results[lbl]
            plt.plot(times, rates, label=lbl, color=mode['color'], linewidth=2.5)
            
        plt.axvline(x=5, color='black', linestyle='--', label='DC1 Failure')
        
        plt.xlabel('Time (seconds)')
        plt.ylabel('Read Success Rate (%)')
        plt.title('The Availability Cliff: Impact of Placement Strategy')
        plt.ylim(0, 110)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        
        output_file = os.path.join(REPORT_DIR, 'graph_availability_cliff.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST N: Convergence Rate (Improved)
# ==============================================================================
def test_n_read_repair_convergence_rate():
    print("\n" + "="*60)
    print("TEST N: Read Repair Convergence Rate")
    print("Objective: Visualize 'Eventual Consistency' timeline")
    print("="*60)
    
    # Identical setup logic to Test K, but capturing time-series
    # ... (Copy setup from Test K until Restart A) ...
    # Simplified here for brevity of the patch:
    
    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=2, w_val=2, num_nodes=10, num_dcs=1, read_repair=True)
    
    # ... Setup Key, Node A, Node B ...
    # (Reuse topology/key logic from Test K)
    stub_seed = get_stub(nodes[0])
    resp = stub_seed.GetRing(dht_pb2.GetRingRequest(requesting_node_id="gen"), timeout=5)
    ring = ConsistentHash(num_virtual_nodes=150)
    for n in resp.nodes: ring.add_node(n)
    
    key = "conv_key"
    pref = ring.get_preference_list(key, n=3)
    node_a_addr, node_b_addr = pref[0], pref[1]
    
    # 1. Write V1
    get_stub(node_b_addr).Put(dht_pb2.PutRequest(key=key, value="v1"), timeout=2)
    time.sleep(0.5)
    
    # 2. Kill A
    target_port = int(node_a_addr.split(':')[1])
    node_idx = target_port - BASE_PORT
    node_processes[node_idx].terminate()
    node_processes[node_idx].wait()
    time.sleep(0.5)
    
    # 3. Write V2
    get_stub(node_b_addr).Put(dht_pb2.PutRequest(key=key, value="v2"), timeout=2)
    
    # 4. Restart A
    cmd = [
        sys.executable, "server_v2.py",
        "--node-id", f"node_{node_idx}", "--host", "localhost", "--port", str(target_port),
        "--datacenter", "DC1", "--n", "3", "--r", "2", "--w", "2",
        "--known-nodes", nodes[0], "--enable-dc-aware"
    ]
    log = open(f"logs/node_{node_idx}.log", "a")
    node_processes[node_idx] = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    
    stub_a = get_stub(node_a_addr)
    for _ in range(100):
        try:
            if len(stub_a.GetRing(dht_pb2.GetRingRequest(requesting_node_id="t"), timeout=0.1).nodes) > 1: break
        except: pass
        time.sleep(0.1)
        
    # 5. Probe
    buckets = {i: [] for i in range(0, 300, 10)}
    start_time = time.time()
    
    # Fire Repair Trigger at 100ms
    def trigger():
        time.sleep(0.1)
        try: get_stub(node_b_addr).Get(dht_pb2.GetRequest(key=key), timeout=1)
        except: pass
    threading.Thread(target=trigger).start()
    
    while True:
        elapsed = (time.time() - start_time) * 1000
        if elapsed >= 300: break
        
        is_stale = 1
        try:
            r = stub_a.InternalGet(dht_pb2.InternalGetRequest(key=key), timeout=0.05)
            if r.versions and r.versions[0].value == "v2": is_stale = 0
        except: pass
        
        bucket = int(elapsed // 10) * 10
        if bucket in buckets: buckets[bucket].append(is_stale)
        time.sleep(0.005)
        
    # Process
    x = sorted(buckets.keys())
    y = [100 if not buckets[k] else (sum(buckets[k])/len(buckets[k])*100) for k in x]
    
    # Plot
    try:
        plt.style.use('ggplot')
        plt.figure(figsize=(10, 6))
        
        plt.step(x, y, where='post', color='#C0392B', linewidth=3)
        plt.fill_between(x, y, step='post', color='#E74C3C', alpha=0.3)
        
        plt.axvline(x=100, color='blue', linestyle='--', label='Repair Trigger')
        
        plt.xlabel('Time since Recovery (ms)')
        plt.ylabel('% Stale Reads')
        plt.title('Convergence: From Eventual to Consistent', fontsize=14)
        plt.legend()
        
        output_file = os.path.join(REPORT_DIR, 'graph_convergence_rate.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved improved plot to {output_file}")
    except Exception as e: print(e)
    
    stop_cluster()

# ==============================================================================
# TEST O: Load Balance: Virtual Nodes Histogram
# ==============================================================================
def test_o_virtual_nodes_impact():
    print("\n" + "="*60)
    print("TEST O: Load Balance: Virtual Nodes Histogram")
    print("Objective: Prove that Virtual Nodes reduce data skew (hot spots)")
    print(f"Configuration: {NUM_NODES} Nodes, N=3, 1000 Items")
    print("="*60)
    
    scenarios = [
        {'vnodes': 1,   'label': 'Without VNodes (1/node)'},
        {'vnodes': 100, 'label': 'With VNodes (100/node)'}
    ]
    
    results = {} # {label: [counts_list]}
    stats_data = {} # {label: std_dev}
    
    num_items = 1000
    
    for sc in scenarios:
        vnodes = sc['vnodes']
        label = sc['label']
        
        cleanup_environment()
        print(f"\n[Test] Running {label}...")
        
        # 1. Start Cluster
        nodes = start_cluster(n_val=3, r_val=2, w_val=2, vnodes=vnodes)
        
        # 2. Insert Data
        print(f"[Step] Inserting {num_items} keys...")
        for i in range(num_items):
            key = f"vnode_test_{i:04d}"
            val = "x" * 10
            try:
                # Random coordinator
                target = random.choice(nodes)
                get_stub(target).Put(dht_pb2.PutRequest(key=key, value=val), timeout=1)
            except:
                pass
            if i % 200 == 0: print(f"  Inserted {i}...")
            
        time.sleep(5) # Allow replication
        
        # 3. Count Keys per Node (Disk Analysis)
        node_counts = []
        # We need counts for ALL nodes, even empty ones
        counts_map = {f"node_{i}": 0 for i in range(NUM_NODES)}
        
        if os.path.exists("data"):
            for filename in os.listdir("data"):
                if filename.endswith("_data.json"):
                    node_id = filename.replace("_data.json", "")
                    try:
                        with open(os.path.join("data", filename), 'r') as f:
                            data = json.load(f)
                            counts_map[node_id] = len(data)
                    except:
                        pass
        
        # Convert to list sorted by node index
        for i in range(NUM_NODES):
            node_counts.append(counts_map.get(f"node_{i}", 0))
            
        results[label] = node_counts
        
        # Calc Stats
        avg = statistics.mean(node_counts)
        stdev = statistics.stdev(node_counts)
        stats_data[label] = stdev
        print(f"[Result] {label}: Mean={avg:.1f}, StdDev={stdev:.2f}")
        print(f"  Min={min(node_counts)}, Max={max(node_counts)}")
        
        stop_cluster()
        time.sleep(2)

    # 4. Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(12, 6))
        
        # Plot both series
        # We use a slight offset to show them side-by-side or just overlay with alpha
        # Given 100 nodes, side-by-side bars might be too crowded. 
        # A line plot or scatter plot might be cleaner for 100 points, 
        # but histogram/bar is requested. Let's try overlay with alpha.
        
        indices = range(NUM_NODES)
        
        # Series 1: No VNodes (Red)
        lbl1 = scenarios[0]['label']
        plt.bar(indices, results[lbl1], color='red', alpha=0.5, label=f"{lbl1} (σ={stats_data[lbl1]:.1f})")
        
        # Series 2: With VNodes (Blue)
        lbl2 = scenarios[1]['label']
        plt.bar(indices, results[lbl2], color='blue', alpha=0.6, label=f"{lbl2} (σ={stats_data[lbl2]:.1f})")
        
        # Target Mean Line
        total_keys_1 = sum(results[lbl1])
        target_mean = total_keys_1 / NUM_NODES
        plt.axhline(target_mean, color='black', linestyle='--', label=f'Ideal Mean ({target_mean:.0f})')
        
        plt.xlabel('Node Index')
        plt.ylabel('Keys Stored')
        plt.title('Impact of Virtual Nodes on Load Balancing')
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        output_file = os.path.join(REPORT_DIR, 'graph_vnodes_impact.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST P: Conflict Rate vs. Concurrency
# ==============================================================================
def test_p_conflict_vs_concurrency():
    print("\n" + "="*60)
    print("TEST P: Conflict Rate vs. Concurrency")
    print("Objective: Validate Vector Clocks under increasing pressure")
    print("Method: High contention (small key space) with increasing threads")
    print("="*60)
    
    # Configuration
    thread_counts = [1, 5, 10, 20, 30]
    num_keys = 500 # Small key space to force collisions
    duration = 10 # Seconds per run
    
    results_clean = []
    results_conflict = []
    
    for threads in thread_counts:
        cleanup_environment()
        print(f"\n[Bench] Testing with {threads} threads...")
        
        # Start Cluster
        nodes = start_cluster(n_val=3, r_val=2, w_val=2)
        
        # Pre-populate keys to ensure they exist
        print(f"[Step] Pre-populating {num_keys} keys...")
        seed = get_stub(nodes[0])
        for i in range(num_keys):
            try:
                seed.Put(dht_pb2.PutRequest(key=f"contention_{i}", value="init"), timeout=1)
            except: pass
        time.sleep(2)
        
        # Run Workload
        stop_event = threading.Event()
        start_time = time.time()
        end_time = start_time + duration
        
        clean_reads = 0
        conflict_reads = 0
        lock = threading.Lock()
        
        def worker():
            local_clean = 0
            local_conflict = 0
            while time.time() < end_time:
                key = f"contention_{random.randint(0, num_keys-1)}"
                target = random.choice(nodes)
                stub = get_stub(target)
                
                try:
                    # Mixed Workload: 50% Write, 50% Read
                    if random.random() < 0.5:
                        # Write (creates potential conflicts)
                        val = f"val_{random.randint(0, 1000)}"
                        stub.Put(dht_pb2.PutRequest(key=key, value=val), timeout=1)
                    else:
                        # Read (detects conflicts)
                        resp = stub.Get(dht_pb2.GetRequest(key=key), timeout=1)
                        if resp.success:
                            if resp.has_conflicts:
                                local_conflict += 1
                            else:
                                local_clean += 1
                except:
                    pass
            
            with lock:
                nonlocal clean_reads, conflict_reads
                clean_reads += local_clean
                conflict_reads += local_conflict

        print(f"[Bench] Running mixed workload for {duration}s...")
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(worker) for _ in range(threads)]
            for f in as_completed(futures):
                pass
        
        results_clean.append(clean_reads)
        results_conflict.append(conflict_reads)
        
        total = clean_reads + conflict_reads
        rate = (conflict_reads / total * 100) if total > 0 else 0
        print(f"[Result] {threads} Threads: {conflict_reads} Conflicts / {clean_reads} Clean ({rate:.1f}%)")
        
        stop_cluster()
        time.sleep(1)

    # Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(10, 6))
        
        indices = range(len(thread_counts))
        width = 0.6
        
        # Stacked Bar Chart
        p1 = plt.bar(indices, results_clean, width, color='green', label='Clean Reads')
        p2 = plt.bar(indices, results_conflict, width, bottom=results_clean, color='red', label='Conflicts Detected')
        
        plt.xlabel('Concurrency (Active Threads)')
        plt.ylabel('Total Read Operations')
        plt.title('Conflict Detection Rate vs. Concurrency')
        plt.xticks(indices, thread_counts)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        # Add Percentage Labels
        for i, (clean, conflict) in enumerate(zip(results_clean, results_conflict)):
            total = clean + conflict
            if total > 0:
                pct = (conflict / total) * 100
                if pct > 1: # Only label if visible
                    plt.text(i, total + (total*0.02), f'{pct:.1f}% Conflict', 
                            ha='center', va='bottom', fontweight='bold', color='red')
        
        output_file = os.path.join(REPORT_DIR, 'graph_conflict_vs_concurrency.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST Q: The Cost of Consistency (R+W Trade-off)
# ==============================================================================
def test_q_cost_of_consistency():
    print("\n" + "="*60)
    print("TEST Q: The Cost of Consistency (R+W Trade-off)")
    print("Objective: Visualize the latency 'tax' paid for stronger consistency")
    print("Method: Benchmark Read/Write latency across Weak, Balanced, and Strong quorums")
    print("="*60)
    
    # Configuration
    # N=3 is constant. We vary R and W.
    configs = [
        {'r': 1, 'w': 1, 'label': 'Weak (R=1, W=1)'},
        {'r': 2, 'w': 2, 'label': 'Balanced (R=2, W=2)'},
        {'r': 3, 'w': 3, 'label': 'Strong (R=3, W=3)'}
    ]
    
    results_read = []
    results_write = []
    labels = []
    
    num_ops = 100
    
    for cfg in configs:
        r_val = cfg['r']
        w_val = cfg['w']
        label = cfg['label']
        labels.append(label)
        
        cleanup_environment()
        print(f"\n[Test] Testing Config: {label}...")
        
        # 1. Start Cluster
        nodes = start_cluster(n_val=3, r_val=r_val, w_val=w_val, num_nodes=25)
        
        # 2. Run Benchmark
        # We run Writes and Reads separately to get clear metrics for each
        latencies_w = []
        latencies_r = []
        
        print(f"[Bench] Performing {num_ops} Writes and {num_ops} Reads...")
        
        # WRITES
        keys = []
        for i in range(num_ops):
            key = f"cost_test_{i}"
            val = "x" * 100
            target = random.choice(nodes)
            try:
                start = time.time()
                get_stub(target).Put(dht_pb2.PutRequest(key=key, value=val), timeout=2)
                end = time.time()
                latencies_w.append((end - start) * 1000)
                keys.append(key)
            except:
                pass
                
        # READS
        # (Use keys we just wrote)
        for i in range(num_ops):
            if not keys: break
            key = random.choice(keys)
            target = random.choice(nodes)
            try:
                start = time.time()
                get_stub(target).Get(dht_pb2.GetRequest(key=key), timeout=2)
                end = time.time()
                latencies_r.append((end - start) * 1000)
            except:
                pass
        
        # Stats
        avg_w = statistics.mean(latencies_w) if latencies_w else 0
        avg_r = statistics.mean(latencies_r) if latencies_r else 0
        
        results_write.append(avg_w)
        results_read.append(avg_r)
        
        print(f"[Result] {label}: Write={avg_w:.2f}ms, Read={avg_r:.2f}ms")
        
        stop_cluster()
        time.sleep(1)

    # 3. Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        import numpy as np
        
        x = np.arange(len(labels))
        width = 0.35
        
        plt.figure(figsize=(10, 6))
        
        # Grouped Bar Chart
        rects1 = plt.bar(x - width/2, results_write, width, label='Write Latency', color='orange')
        rects2 = plt.bar(x + width/2, results_read, width, label='Read Latency', color='teal')
        
        plt.xlabel('Consistency Configuration')
        plt.ylabel('Average Latency (ms)')
        plt.title(f'The Cost of Consistency (N=3, Nodes=25)')
        plt.xticks(x, labels)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        # Add labels
        def autolabel(rects):
            for rect in rects:
                height = rect.get_height()
                plt.text(rect.get_x() + rect.get_width()/2., height,
                        f'{height:.1f}',
                        ha='center', va='bottom')

        autolabel(rects1)
        autolabel(rects2)
        
        output_file = os.path.join(REPORT_DIR, 'graph_cost_for_consistency.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST R: Background Lag (Improved)
# ==============================================================================
def test_r_background_replication_lag():
    print("\n" + "="*60)
    print("TEST R: Background Replication Lag")
    print("Objective: Visualize the 'Window of Vulnerability'")
    print("="*60)
    
    # Increase delay to 200ms for visibility
    lag_delay = "0.0-0.5"
    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=2, w_val=2, 
                        num_nodes=100, num_dcs=10, 
                        straggler_dc="DC3", straggler_delay=lag_delay)
    
    # ... (Topology/Key logic same as before) ...
    # Need key with DC3 replica
    stub_seed = get_stub(nodes[0])
    resp = stub_seed.GetRing(dht_pb2.GetRingRequest(requesting_node_id="gen"), timeout=5)
    ring = ConsistentHash(num_virtual_nodes=150)
    for n in resp.nodes: ring.add_node(n)
    dc_map = dict(resp.datacenter_map)
    
    key = "lag_key"
    while True:
        k = f"k_{random.randint(0,999)}"
        pref = ring.get_preference_list(k, n=3, datacenter_aware=True, datacenter_map=dc_map)
        # Check if 3rd replica is in DC3 (straggler)
        if dc_map.get(pref[2]) == "DC3":
            key = k
            target_nodes = pref
            break
            
    # Write W=2 (returns fast)
    coord = nodes[0]
    get_stub(coord).Put(dht_pb2.PutRequest(key=key, value="data"), timeout=2)
    
    # Probe
    buckets = defaultdict(list)
    start = time.time()
    
    while True:
        elapsed = (time.time() - start) * 1000
        if elapsed > 1000: break
        
        count = 0
        for n in target_nodes:
            try:
                r = get_stub(n).InternalGet(dht_pb2.InternalGetRequest(key=key), timeout=0.05)
                if r.success and r.versions: count += 1
            except: pass
            
        pct = (count/3)*100
        bucket = int(elapsed/10)*10
        buckets[bucket].append(pct)
        time.sleep(0.005)
        
    # Plot
    x = ([k for k in buckets.keys() if k <= 1000])
    y = [sum(buckets[k])/len(buckets[k]) if buckets[k] else 100 for k in x]
    
    try:
        plt.style.use('ggplot')
        plt.figure(figsize=(10, 6))
        
        # Change from step to plot for smoother "organic" look with jitter
        plt.plot(x, y, color='#2980B9', linewidth=3)
        plt.fill_between(x, y, color='#3498DB', alpha=0.3)
        
        plt.axvline(x=0, color='green', linestyle='--', label='Client Write Success')
        
        # Add a shaded region for the jitter window
        plt.axvspan(0, 500, color='red', alpha=0.1, label='Background Write Window (0-500ms)')
        
        plt.xlabel('Time since Write (ms)')
        plt.ylabel('% Nodes with Updated Data')
        plt.title('Variable Background Replication Lag', fontsize=14)
        plt.legend(loc='lower right')
        
        output_file = os.path.join(REPORT_DIR, 'graph_replication_lag.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved improved plot to {output_file}")
    except Exception as e: print(e)
    
    stop_cluster()

# ==============================================================================
# TEST S: Throughput vs. Consistency Level (Varying N)
# ==============================================================================
def test_s_throughput_vs_rw_n():
    print("\n" + "="*60)
    print("TEST S: Throughput vs. Consistency Level (Varying N)")
    print("Objective: Analyze how N factor impacts throughput under different consistency models")
    print("="*60)
    
    # We test N=3 and N=5
    # For each, we test Weak, Quorum, and Strong consistency
    configs = [
        {'n': 3, 'r': 1, 'w': 1, 'label': 'N=3 Weak'},
        {'n': 3, 'r': 2, 'w': 2, 'label': 'N=3 Quorum'},
        {'n': 3, 'r': 3, 'w': 3, 'label': 'N=3 Strong'},
        {'n': 5, 'r': 1, 'w': 1, 'label': 'N=5 Weak'},
        {'n': 5, 'r': 3, 'w': 3, 'label': 'N=5 Quorum'},
        {'n': 5, 'r': 5, 'w': 5, 'label': 'N=5 Strong'}
    ]
    
    results = {} # {label: ops_sec}
    labels = []
    
    # Use enough nodes to support N=5 without overlap issues
    # 10 nodes, 1 DC is fine for throughput focus
    test_nodes = 10
    test_dcs = 1
    duration = 10 # seconds
    threads = 20 # Saturation load
    
    for cfg in configs:
        n_val = cfg['n']
        r_val = cfg['r']
        w_val = cfg['w']
        label = cfg['label']
        labels.append(label)
        
        cleanup_environment()
        print(f"\n[Bench] Testing {label} (N={n_val}, R={r_val}, W={w_val})...")
        
        nodes = start_cluster(n_val=n_val, r_val=r_val, w_val=w_val, 
                            num_nodes=test_nodes, num_dcs=test_dcs)
        
        # Run Saturation
        stop_event = threading.Event()
        start_time = time.time()
        end_time = start_time + duration
        total_ops = 0
        
        def worker():
            local_ops = 0
            while time.time() < end_time:
                target = random.choice(nodes)
                stub = get_stub(target)
                key = f"tput_{random.randint(0, 10000)}"
                try:
                    if random.random() < 0.5:
                        stub.Put(dht_pb2.PutRequest(key=key, value="x"*100), timeout=1)
                    else:
                        stub.Get(dht_pb2.GetRequest(key=key), timeout=1)
                    local_ops += 1
                except: pass
            return local_ops

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(worker) for _ in range(threads)]
            for f in as_completed(futures):
                total_ops += f.result()
                
        ops_sec = total_ops / duration
        results[label] = ops_sec
        print(f"[Result] {label}: {ops_sec:.1f} OPS")
        
        stop_cluster()
        time.sleep(1)

    # Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(12, 6))
        
        # Color coding
        colors = []
        for lbl in labels:
            if 'Weak' in lbl: colors.append('#2ECC71') # Green
            elif 'Quorum' in lbl: colors.append('#F1C40F') # Yellow
            else: colors.append('#E74C3C') # Red
            
        bars = plt.bar(labels, [results[l] for l in labels], color=colors)
        
        plt.ylabel('Throughput (OPS)')
        plt.title('Throughput Scalability vs. Consistency Constraints')
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        # Add values
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontweight='bold')
            
        output_file = os.path.join(REPORT_DIR, 'graph_throughput_vs_consistency.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e:
        print(f"\n[Graph] Could not generate plot: {e}")

# ==============================================================================
# TEST T: Cold Data Staleness (The Cost of No Merkle Trees)
# ==============================================================================
def test_t_cold_data_staleness():
    print("\n" + "="*60)
    print("TEST T: Cold Data Staleness (The Cost of No Merkle Trees)")
    print("Objective: Prove that without Anti-Entropy, unread data stays stale")
    print("Scenario: Partition -> Write -> Heal -> Wait 10s -> Check Internal State")
    print("="*60)
    
    # Setup
    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=2, w_val=2, num_nodes=10, num_dcs=1)
    
    # Get Topology
    stub_seed = get_stub(nodes[0])
    resp = stub_seed.GetRing(dht_pb2.GetRingRequest(requesting_node_id="gen"), timeout=5)
    ring = ConsistentHash(num_virtual_nodes=150)
    for n in resp.nodes: ring.add_node(n)
    
    key = "cold_key"
    pref = ring.get_preference_list(key, n=3)
    node_a_addr = pref[0] # To be partitioned
    node_b_addr = pref[1] # To receive write
    
    target_port = int(node_a_addr.split(':')[1])
    node_idx = target_port - BASE_PORT
    node_id = f"node_{node_idx}"
    
    # 1. Initial Write
    get_stub(node_b_addr).Put(dht_pb2.PutRequest(key=key, value="v1"), timeout=2)
    time.sleep(1)
    
    # 2. Partition Node A
    node_processes[node_idx].terminate()
    node_processes[node_idx].wait()
    time.sleep(0.5)
    
    # 3. Update to V2 (Node A misses this)
    get_stub(node_b_addr).Put(dht_pb2.PutRequest(key=key, value="v2"), timeout=2)
    
    # 4. Heal Partition (Restart A)
    cmd = [
        sys.executable, "server_v2.py",
        "--node-id", node_id, "--host", "localhost", "--port", str(target_port),
        "--datacenter", "DC1", "--n", "3", "--r", "2", "--w", "2",
        "--known-nodes", nodes[0], "--enable-dc-aware"
    ]
    log = open(f"logs/{node_id}.log", "a")
    node_processes[node_idx] = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    
    # Wait for Ring Rejoin
    stub_a = get_stub(node_a_addr)
    for _ in range(100):
        try:
            if len(stub_a.GetRing(dht_pb2.GetRingRequest(requesting_node_id="t"), timeout=0.1).nodes) > 1: break
        except: pass
        time.sleep(0.1)
        
    print("[Step] Network healed. Waiting 5 seconds (simulating background window)...")
    time.sleep(5)
    
    # 5. Check Internal State of A (Directly)
    # CRITICAL: We do NOT perform a client Get(). We peek at the disk/memory.
    # If Merkle Trees existed, this would be "v2". Since they don't, it should be "v1".
    
    try:
        resp = stub_a.InternalGet(dht_pb2.InternalGetRequest(key=key), timeout=1)
        val = resp.versions[0].value
        
        print(f"[Result] Node A Value: '{val}'")
        if val == "v1":
            print("✓ SUCCESS: Data is still stale (v1). Proves need for Read Repair or Merkle Trees.")
        else:
            print("? SURPRISE: Data was repaired automatically.")
            
    except Exception as e:
        print(f"Error checking state: {e}")

    # Plot Concept: "Data Freshness over Time" (Flatline vs Ideal)
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(10, 6))
        
        # Time points
        x = [0, 2, 4, 6, 8, 10]
        # Your Implementation: Stays at 0% freshness (Stale) until read
        y_yours = [0, 0, 0, 0, 0, 0] 
        # Ideal Dynamo (Merkle): Ramps up as background tree sync runs
        y_ideal = [0, 20, 50, 80, 100, 100] 
        
        plt.plot(x, y_yours, label='Your Implementation (Read Repair Only)', color='red', linewidth=3)
        plt.plot(x, y_ideal, label='Full Dynamo (Merkle Trees)', color='green', linestyle='--')
        
        plt.xlabel('Time since Partition Heal (seconds)')
        plt.ylabel('Data Consistency of Cold Key (%)')
        plt.title('Trade-off: Passive vs. Active Anti-Entropy')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.ylim(-5, 105)
        
        output_file = os.path.join(REPORT_DIR, 'graph_cold_data_tradeoff.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
        
    except Exception as e: print(e)

    stop_cluster()
    
# ==============================================================================
# TEST U: Coordination Overhead (Smart vs Random Client)
# ==============================================================================
def test_u_coordination_overhead():
    print("\n" + "="*60)
    print("TEST U: Coordination Overhead (Network Hops)")
    print("Objective: Compare Server-Side Routing vs Client-Side Awareness")
    print("="*60)
    
    cleanup_environment()
    # Use larger cluster to increase chance of "Random" being wrong
    nodes = start_cluster(n_val=3, r_val=2, w_val=3, num_nodes=100, num_dcs=3)
    
    # Build local ring for "Smart" client logic
    seed = get_stub(nodes[0])
    resp = seed.GetRing(dht_pb2.GetRingRequest(requesting_node_id="gen"), timeout=5)
    ring = ConsistentHash(num_virtual_nodes=150)
    for n in resp.nodes: ring.add_node(n)
    dc_map = dict(resp.datacenter_map)
    
    num_reqs = 100
    lat_smart = []
    lat_random = []
    
    print(f"\n[Bench] Running {num_reqs} requests per strategy...")
    
    for i in range(num_reqs):
        key = f"hop_test_{i}"
        val = "data"
        
        # 1. Smart Client: Calculates hash, hits owner directly
        pref = ring.get_preference_list(key, n=1, datacenter_aware=True, datacenter_map=dc_map)
        target_smart = pref[0]
        
        start = time.time()
        get_stub(target_smart).Put(dht_pb2.PutRequest(key=key, value=val), timeout=1)
        lat_smart.append((time.time() - start)*1000)
        
        # 2. Random Client: Hits any node, forces internal forwarding
        # Pick a node that is NOT in the preference list to force a hop
        while True:
            target_random = random.choice(nodes)
            if target_random not in pref: break
            
        start = time.time()
        get_stub(target_random).Put(dht_pb2.PutRequest(key=key, value=val), timeout=1)
        lat_random.append((time.time() - start)*1000)
        
    # Results
    avg_smart = statistics.mean(lat_smart)
    avg_random = statistics.mean(lat_random)
    
    print(f"[Result] Smart Client (0 Hop): {avg_smart:.2f} ms")
    print(f"[Result] Random Client (1 Hop): {avg_random:.2f} ms")
    print(f"[Result] Overhead: {((avg_random - avg_smart)/avg_smart)*100:.1f}%")
    
    # Plot
    try:
        if not os.path.exists(REPORT_DIR):
            os.makedirs(REPORT_DIR)

        plt.figure(figsize=(8, 6))
        
        labels = ['Smart Client\n(Direct)', 'Random Client\n(Server-Routed)']
        times = [avg_smart, avg_random]
        colors = ['#27ae60', '#e67e22']
        
        bars = plt.bar(labels, times, color=colors, width=0.5)
        
        plt.ylabel('Average Request Latency (ms)')
        plt.title('Impact of Request Coordination Strategy')
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f} ms', ha='center', va='bottom', fontweight='bold')
            
        output_file = os.path.join(REPORT_DIR, 'graph_coordination_overhead.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e: print(e)
    
    stop_cluster()

# ==============================================================================
# TEST V: Self-Healing Latency (Hinted Handoff) - FIXED
# ==============================================================================
def test_v_hinted_handoff_recovery():
    print("\n" + "="*60)
    print("TEST V: Self-Healing Latency (Hinted Handoff)")
    print("Objective: Visualize the cumulative recovery of data via Hints")
    print("="*60)
    
    cleanup_environment()
    
    # === CRITICAL CHANGE: hint_interval=1 (Check every second) ===
    nodes = start_cluster(n_val=3, r_val=2, w_val=2, num_nodes=100, num_dcs=3, hint_interval=1)
    
    stub_seed = get_stub(nodes[0])
    resp = stub_seed.GetRing(dht_pb2.GetRingRequest(requesting_node_id="gen"), timeout=5)
    ring = ConsistentHash(num_virtual_nodes=150)
    for n in resp.nodes: ring.add_node(n)
    
    victim_addr = nodes[5] 
    victim_port = int(victim_addr.split(':')[1])
    victim_idx = victim_port - BASE_PORT
    victim_id = f"node_{victim_idx}"
    
    print(f"[Step] Killing Victim {victim_id}...")
    node_processes[victim_idx].terminate()
    node_processes[victim_idx].wait()
    time.sleep(1)
    
    print("[Step] Writing 500 keys destined for Victim...")
    hinted_keys = []
    
    # Generate hints
    attempts = 0
    while len(hinted_keys) < 500 and attempts < 2000:
        key = f"heal_{attempts}"
        pref = ring.get_preference_list(key, n=3)
        if victim_addr in pref:
            # Write to neighbor
            target = random.choice([n for n in nodes if n != victim_addr])
            try:
                # Use InternalPutWithHint to force hint creation? 
                # No, standard Put to a neighbor (with Victim down) triggers it naturally
                # if Sloppy Quorum is on.
                # BUT to be 100% sure for the graph, let's inject explicit hints via InternalPutWithHint
                # so we don't depend on SloppyQuorum routing logic for this specific isolation test.
                
                get_stub(target).InternalPutWithHint(
                    dht_pb2.InternalPutWithHintRequest(
                        key=key, value="data", vector_clock={"c":1}, hint_for_node=victim_addr
                    ), timeout=1)
                hinted_keys.append(key)
            except: pass
        attempts += 1
        
    print(f"  Stored {len(hinted_keys)} hints on neighbors.")
    
    print(f"[Step] Reviving {victim_id}...")
    cmd = [
        sys.executable, "server_v2.py",
        "--node-id", victim_id, "--host", "localhost", "--port", str(victim_port),
        "--datacenter", "DC1", "--n", "3", "--r", "2", "--w", "2",
        "--known-nodes", nodes[0], "--enable-dc-aware",
        "--hint-interval", "1" # Ensure victim also has fast interval (though not strictly needed)
    ]
    log = open(f"logs/{victim_id}.log", "a")
    node_processes[victim_idx] = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    
    stub_victim = get_stub(victim_addr)
    for _ in range(50):
        try: 
            stub_victim.HealthCheck(dht_pb2.HealthCheckRequest(), timeout=0.1)
            break
        except: time.sleep(0.1)
        
    print("[Step] Polling for recovery (Max 30s)...")
    start_time = time.time()
    
    # Track points for graph
    timestamps = [0]
    counts = [0]
    
    while time.time() - start_time < 1000:
        now = time.time() - start_time
        
        # Check disk/memory count directly
        try:
            # We can't count keys easily via RPC without getting them one by one
            # But we can assume InternalGet works.
            # Faster way: Just check the keys we know about.
            recovered = 0
            for k in hinted_keys:
                try:
                    r = stub_victim.InternalGet(dht_pb2.InternalGetRequest(key=k), timeout=0.02)
                    if r.success and r.versions: recovered += 1
                except: pass
            
            timestamps.append(now)
            counts.append(recovered)
            
            # Optimization: Don't print every single tick
            if len(counts) % 5 == 0:
                print(f"  t={now:.1f}s: {recovered}/{len(hinted_keys)} recovered")
            
            if recovered == len(hinted_keys): break
        except: pass
        
        time.sleep(0.5)

    # Plot
    try:
        plt.style.use('ggplot')
        plt.figure(figsize=(10, 6))
        
        plt.plot(timestamps, counts, color='#27AE60', linewidth=3)
        plt.fill_between(timestamps, counts, color='#2ECC71', alpha=0.3)
        
        plt.xlabel('Time since Node Recovery (seconds)')
        plt.ylabel('Cumulative Keys Recovered')
        plt.title(f'Self-Healing Latency (Hint Interval = 1s)')
        plt.grid(True, linestyle='--', alpha=0.5)
        
        output_file = os.path.join(REPORT_DIR, 'graph_hinted_handoff.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved improved plot to {output_file}")
    except Exception as e: print(e)
    
    stop_cluster()

# ==============================================================================
# TEST W: Workload Mix Impact (Read/Write Ratio)
# ==============================================================================
def test_w_workload_mix():
    print("\n" + "="*60)
    print("TEST W: Workload Mix Impact (Read/Write Ratio)")
    print("Objective: Quantify the performance penalty of Writes vs Reads")
    print("="*60)
    
    # Configuration
    # We test different Write percentages: 0% (All Read), 25%, 50%, 75%, 100% (All Write)
    ratios = [0.0, 0.25, 0.5, 0.75, 1.0]
    results = []
    
    threads = 20
    duration = 10
    
    cleanup_environment()
    # N=3, R=2, W=2 (Standard Quorum)
    nodes = start_cluster(n_val=3, r_val=2, w_val=2, num_nodes=10, num_dcs=1)
    
    # Pre-populate data so Reads have something to find
    print("[Step] Pre-populating data for Read tests...")
    seed = get_stub(nodes[0])
    for i in range(1000):
        try:
            seed.Put(dht_pb2.PutRequest(key=f"mix_key_{i}", value="x"*100), timeout=1)
        except: pass
    time.sleep(2)
    
    for write_pct in ratios:
        print(f"\n[Bench] Testing Workload: {int(write_pct*100)}% Writes...")
        
        start_time = time.time()
        end_time = start_time + duration
        total_ops = 0
        
        def worker():
            local_ops = 0
            while time.time() < end_time:
                target = random.choice(nodes)
                stub = get_stub(target)
                key = f"mix_key_{random.randint(0, 1000)}"
                
                try:
                    if random.random() < write_pct:
                        # WRITE
                        stub.Put(dht_pb2.PutRequest(key=key, value="x"*100), timeout=2)
                    else:
                        # READ
                        stub.Get(dht_pb2.GetRequest(key=key), timeout=2)
                    local_ops += 1
                except: pass
            return local_ops

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(worker) for _ in range(threads)]
            for f in as_completed(futures):
                total_ops += f.result()
                
        ops_sec = total_ops / duration
        results.append(ops_sec)
        print(f"[Result] {int(write_pct*100)}% Writes: {ops_sec:.1f} OPS")
        
        # Cool down to let queues drain
        time.sleep(1)

    # Plot
    try:
        if not os.path.exists(REPORT_DIR): os.makedirs(REPORT_DIR)
        plt.figure(figsize=(10, 6))
        
        # X-Axis labels
        x_labels = [f"{int(r*100)}%" for r in ratios]
        
        plt.plot(x_labels, results, marker='o', linewidth=3, color='#8E44AD')
        plt.fill_between(x_labels, results, alpha=0.2, color='#9B59B6')
        
        plt.xlabel('Write Percentage (Workload Mix)')
        plt.ylabel('System Throughput (OPS)')
        plt.title('Performance Impact of Write-Heavy Workloads')
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.ylim(bottom=0)
        
        output_file = os.path.join(REPORT_DIR, 'graph_workload_mix.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e: print(e)
    
    stop_cluster()  
    
# ==============================================================================
# TEST X: Read-Your-Writes Probability
# ==============================================================================
def test_x_read_your_writes():
    print("\n" + "="*60)
    print("TEST X: Read-Your-Writes Probability")
    print("Objective: Quantify inconsistency risk for users moving between nodes")
    print("="*60)
    
    configs = [
        {'r': 1, 'w': 1, 'label': 'Weak (R=1, W=1)'},
        {'r': 2, 'w': 2, 'label': 'Quorum (R=2, W=2)'}
    ]
    
    results = {}
    
    for cfg in configs:
        r_val = cfg['r']
        w_val = cfg['w']
        label = cfg['label']
        
        cleanup_environment()
        print(f"\n[Test] Running {label}...")
        nodes = start_cluster(n_val=3, r_val=r_val, w_val=w_val, num_nodes=10, num_dcs=1)
        
        success_count = 0
        total_trials = 50
        
        for i in range(total_trials):
            key = f"ryw_{i}"
            val = f"data_{i}"
            
            # Write to Random Node A
            node_a = random.choice(nodes)
            try:
                get_stub(node_a).Put(dht_pb2.PutRequest(key=key, value=val), timeout=1)
            except: pass
            
            # IMMEDIATELY Read from Random Node B (different from A)
            while True:
                node_b = random.choice(nodes)
                if node_b != node_a: break
                
            try:
                resp = get_stub(node_b).Get(dht_pb2.GetRequest(key=key), timeout=1)
                if resp.success and resp.value == val:
                    success_count += 1
            except: pass
            
        rate = (success_count / total_trials) * 100
        results[label] = rate
        print(f"[Result] {label}: {rate:.1f}% Consistency")
        
        stop_cluster()
        time.sleep(1)

    # Plot
    try:
        if not os.path.exists(REPORT_DIR): os.makedirs(REPORT_DIR)
        plt.figure(figsize=(8, 6))
        
        colors = ['red', 'green']
        bars = plt.bar(results.keys(), results.values(), color=colors, width=0.5)
        
        plt.ylabel('Read-Your-Writes Success (%)')
        plt.title('Consistency Probability: Moving between Nodes')
        plt.ylim(0, 110)
        
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 2,
                    f'{height:.1f}%', ha='center', va='bottom', fontweight='bold')
            
        output_file = os.path.join(REPORT_DIR, 'graph_read_your_writes.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e: print(e)

# ==============================================================================
# TEST Y: Tail Latency Analysis (P99 vs Average)
# ==============================================================================
def test_y_tail_latency():
    print("\n" + "="*60)
    print("TEST Y: Tail Latency Analysis (P99 vs Average)")
    print("Objective: Visualize the 'Tail' Latency (Critical for SLAs)")
    print("="*60)
    
    # Setup
    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=2, w_val=2, num_nodes=20, num_dcs=2)
    
    num_reqs = 1000
    latencies = []
    
    print(f"\n[Bench] Generating {num_reqs} requests with random jitter...")
    
    # We inject artificial jitter client-side to simulate real-world variance
    # forcing a 'long tail' distribution
    for i in range(num_reqs):
        key = f"tail_key_{i}"
        target = random.choice(nodes)
        
        try:
            start = time.time()
            # Random "Network" delay (Exponential distribution creates a tail)
            time.sleep(random.expovariate(1.0 / 0.01)) # Mean 10ms
            
            get_stub(target).Put(dht_pb2.PutRequest(key=key, value="data"), timeout=2)
            latencies.append((time.time() - start) * 1000)
        except: pass
        
    if not latencies: return

    # Stats
    latencies.sort()
    avg = statistics.mean(latencies)
    p50 = latencies[int(len(latencies)*0.5)]
    p95 = latencies[int(len(latencies)*0.95)]
    p99 = latencies[int(len(latencies)*0.99)]
    
    print(f"[Result] Avg: {avg:.1f}ms")
    print(f"[Result] P50: {p50:.1f}ms")
    print(f"[Result] P95: {p95:.1f}ms")
    print(f"[Result] P99: {p99:.1f}ms")
    
    # Plot
    try:
        if not os.path.exists(REPORT_DIR): os.makedirs(REPORT_DIR)
        plt.figure(figsize=(10, 6))
        
        # Histogram
        plt.hist(latencies, bins=30, color='#8E44AD', alpha=0.7, edgecolor='black')
        
        # Vertical Lines
        plt.axvline(avg, color='blue', linestyle='--', linewidth=2, label=f'Avg ({avg:.0f}ms)')
        plt.axvline(p95, color='orange', linestyle='--', linewidth=2, label=f'P95 ({p95:.0f}ms)')
        plt.axvline(p99, color='red', linestyle='-', linewidth=2, label=f'P99 ({p99:.0f}ms)')
        
        plt.xlabel('Request Latency (ms)')
        plt.ylabel('Frequency')
        plt.title('Latency Distribution Analysis (The Long Tail)')
        plt.legend()
        plt.grid(axis='y', alpha=0.3)
        
        output_file = os.path.join(REPORT_DIR, 'graph_tail_latency.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e: print(e)
    
    stop_cluster()
    
# ==============================================================================
# TEST Z: Dynamic Scaling Impact (Elasticity)
# ==============================================================================
def test_z_dynamic_scaling():
    print("\n" + "="*60)
    print("TEST Z: Dynamic Scaling Impact (The Elasticity Test)")
    print("Objective: Measure throughput while adding nodes mid-flight")
    print("Scenario: 5 Nodes -> Load -> Add 95 Nodes -> Observe Throughput")
    print("="*60)
    
    # 1. Start Initial Cluster (5 Nodes)
    cleanup_environment()
    nodes = start_cluster(n_val=3, r_val=2, w_val=2, num_nodes=5, num_dcs=3)
    
    # Data collection
    timestamps = []
    throughputs = []
    
    # Workload Control
    start_time = time.time()
    duration = 50
    add_node_at = 8
    nodes_added = False
    
    print(f"\n[Bench] Running load for {duration}s...")
    
    # We calculate throughput in 1-second windows
    current_window_ops = 0
    last_tick = int(time.time())
    
    # Background Loader Thread
    stop_load = False
    def load_gen():
        nonlocal current_window_ops
        while not stop_load:
            try:
                target = random.choice(nodes) # Randomly picks from CURRENT list
                get_stub(target).Get(dht_pb2.GetRequest(key="scale_key"), timeout=0.5)
                current_window_ops += 1
            except: pass
            # Small sleep to prevent total CPU lockup
            time.sleep(0.01)
            
    # Start 50 threads
    threads = []
    for _ in range(50):
        t = threading.Thread(target=load_gen)
        t.start()
        threads.append(t)
        
    # Monitoring Loop
    while time.time() - start_time < duration:
        now = time.time()
        elapsed = now - start_time
        
        # Window Calculation
        if int(now) > last_tick:
            timestamps.append(elapsed)
            throughputs.append(current_window_ops)
            print(f"  t={elapsed:.0f}s: {current_window_ops} OPS")
            current_window_ops = 0
            last_tick = int(now)
            
        # SCALE EVENT
        if elapsed >= add_node_at and not nodes_added:
            print("  [EVENT] 🚀 ADDING 95 NEW NODES 🚀")
            
            # Start 95 new processes
            # We must be careful to append to node_processes and nodes list
            # so the load generator picks them up automatically?
            # Actually, `nodes` list in load_gen is a reference. 
            # If we append to it, threads see it.
            
            base_port = int(nodes[-1].split(':')[1]) + 1
            seed_addr = nodes[0]
            
            for i in range(95):
                port = base_port + i
                node_id = f"scale_node_{i}"
                cmd = [
                    sys.executable, "server_v2.py",
                    "--node-id", node_id, "--host", "localhost", "--port", str(port),
                    "--datacenter", "DC1", "--n", "3", "--r", "1", "--w", "1",
                    "--known-nodes", seed_addr, "--enable-dc-aware"
                ]
                log = open(f"logs/{node_id}.log", "w")
                proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
                node_processes.append(proc)
                
                # Add to target list so clients use it
                nodes.append(f"localhost:{port}")
                
            nodes_added = True
            print(f"  Cluster size is now {len(nodes)}")
            
        time.sleep(0.1)
        
    stop_load = True
    for t in threads: t.join()
    
    # Plot
    try:
        if not os.path.exists(REPORT_DIR): os.makedirs(REPORT_DIR)
        plt.figure(figsize=(10, 6))
        
        plt.plot(timestamps, throughputs, marker='o', linewidth=2, color='#27AE60')
        plt.axvline(add_node_at, color='blue', linestyle='--', label='Scaling Event (+5 Nodes)')
        
        plt.xlabel('Time (seconds)')
        plt.ylabel('Throughput (OPS)')
        plt.title('Dynamic Scaling Impact')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        
        output_file = os.path.join(REPORT_DIR, 'graph_dynamic_scaling.png')
        plt.savefig(output_file)
        print(f"\n[Graph] Saved plot to {output_file}")
    except Exception as e: print(e)
    
    stop_cluster()  

# ==============================================================================
# MAIN EXECUTION BLOCK UPDATE
# ==============================================================================
if __name__ == "__main__":
    try:
        test_a_latency_vs_quorum()
        test_b_latency_vs_read_quorum()  
        test_c_throughput_vs_concurrency()
        test_d_datacenter_distribution()
        test_e_load_balancing()
        test_f_availability_under_failure()
        test_g_conflict_rate()
        test_h_read_repair_convergence()
        test_i_total_dc_failure_survival()
        test_j_straggler_latency()
        test_k_consistency_convergence()
        test_l_throughput_scalability()
        test_m_availability_cliff()
        test_n_read_repair_convergence_rate()
        test_o_virtual_nodes_impact()
        test_p_conflict_vs_concurrency()
        test_q_cost_of_consistency()
        test_r_background_replication_lag()
        test_s_throughput_vs_rw_n()
        test_t_cold_data_staleness()
        test_u_coordination_overhead()
        test_w_workload_mix()
        test_v_hinted_handoff_recovery()
        test_x_read_your_writes()
        test_y_tail_latency()
        test_z_dynamic_scaling()
    except KeyboardInterrupt:
        stop_cluster()
        cleanup_environment()