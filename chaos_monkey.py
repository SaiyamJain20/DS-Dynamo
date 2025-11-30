"""
Chaos Manager for Datacenter Failures & Traffic Generation
==========================================================
1. Randomly kills and revives entire Datacenters (groups of nodes).
2. Continuously sends random PUT/GET requests to simulate client traffic.
"""

import time
import random
import os
import signal
import subprocess
import sys
import threading
import string
import grpc
import dht_pb2
import dht_pb2_grpc

# Configuration matches the demo cluster
BASE_PORT = 50051
NUM_NODES = 50
NUM_DCS = 5

# --- HELPER FUNCTIONS ---

def get_nodes_in_dc(target_dc_idx):
    """
    Returns list of node indices belonging to a specific DC (1-based index).
    Mapping logic matches start_demo.py: Node i -> DC (i % 3) + 1
    """
    nodes = []
    for i in range(NUM_NODES):
        dc_idx = (i % NUM_DCS) + 1
        if dc_idx == target_dc_idx:
            nodes.append(i)
    return nodes

def kill_dc(dc_idx):
    """Kill all process IDs associated with nodes in this DC."""
    nodes = get_nodes_in_dc(dc_idx)
    print(f"\n[Chaos] 💥 KILLING DATACENTER {dc_idx} (Nodes: {nodes})")
    
    for node_idx in nodes:
        node_id = f"node_{node_idx}"
        pid_file = f"pids/{node_id}.pid"
        
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                os.kill(pid, signal.SIGTERM)
                os.remove(pid_file) # Clean up so we know it's dead
            except ProcessLookupError:
                pass # Already dead
            except Exception as e:
                print(f"  Failed to kill {node_id}: {e}")
    return nodes

def revive_dc(dc_idx, nodes):
    """Restart the server processes for this DC."""
    print(f"[Chaos] ✨ REVIVING DATACENTER {dc_idx}")
    
    for node_idx in nodes:
        port = BASE_PORT + node_idx
        node_id = f"node_{node_idx}"
        dc = f"DC{dc_idx}"
        
        # Command must match start_demo.py exactly
        cmd = [
            sys.executable, "server_v2.py",
            "--node-id", node_id,
            "--host", "localhost",
            "--port", str(port),
            "--datacenter", dc,
            "--n", "3", "--r", "2", "--w", "2",
            "--enable-dc-aware"
        ]
        
        # Add known-nodes arg (Node 0 is seed, doesn't need it, but others do)
        # We point everyone to localhost:50051 (Node 0)
        if node_idx != 0: 
             cmd.extend(["--known-nodes", f"localhost:{BASE_PORT}"])

        # Append to log, don't overwrite
        log_file = open(f"logs/{node_id}.log", "a")
        p = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        
        # Save new PID
        with open(f"pids/{node_id}.pid", "w") as f:
            f.write(str(p.pid))
            
        time.sleep(0.5) # Stagger startups slightly

# --- TRAFFIC GENERATOR THREAD ---

def get_stub(node_address):
    try:
        channel = grpc.insecure_channel(node_address)
        return dht_pb2_grpc.DHTServiceStub(channel)
    except: return None

def random_string(length=8):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(length))

def run_traffic_gen():
    """Background thread that sends PUT/GET requests."""
    print("[Traffic] Generator started...")
    keys = [f"key_{i}" for i in range(100)]
    
    nodes_addr = [f"localhost:{BASE_PORT + i}" for i in range(NUM_NODES)]
    
    while True:
        try:
            # Pick random target
            target = random.choice(nodes_addr)
            stub = get_stub(target)
            if not stub: continue
            
            # Pick random key/op
            key = random.choice(keys)
            
            # 70% Write, 30% Read
            if random.random() < 0.7:
                val = f"val_{random_string(5)}"
                val_disp = f"{val} @ {time.strftime('%H:%M:%S')}"
                resp = stub.Put(dht_pb2.PutRequest(key=key, value=val_disp), timeout=1)
                status = "SUCCESS" if resp.success else "FAIL"
                # print(f"[Traffic] PUT {key} -> {target} : {status}")
            else:
                resp = stub.Get(dht_pb2.GetRequest(key=key), timeout=1)
                status = "FOUND" if resp.success else "MISS"
                # print(f"[Traffic] GET {key} <- {target} : {status}")
                
        except Exception:
            # Silence errors (expected during Chaos)
            pass
            
        time.sleep(0.2) # Fast traffic

# --- MAIN CHAOS LOOP ---

def run():
    print("😈 DATACENTER CHAOS MANAGER STARTED")
    print(f"Managing {NUM_NODES} nodes across {NUM_DCS} Datacenters.")
    print("Traffic Generator running in background.")
    print("Press Ctrl+C to stop.")
    
    # Start Traffic Thread
    t = threading.Thread(target=run_traffic_gen, daemon=True)
    t.start()
    
    try:
        while True:
            # 1. Wait for stability
            print("\n[Chaos] Waiting 15s for stability...")
            time.sleep(15)
            
            # 2. Pick a random DC to kill (2 or 3)
            # Avoid killing DC1 (contains Seed Node 0) to keep dashboard discovery stable
            target_dc = random.choice([2, 5])
            
            # 3. Kill
            killed_nodes = kill_dc(target_dc)
            
            # 4. Leave dead for a while
            outage_duration = random.randint(5, 25)
            print(f"[Chaos] {len(killed_nodes)} nodes offline for {outage_duration}s...")
            time.sleep(outage_duration)
            
            # 5. Revive
            revive_dc(target_dc, killed_nodes)
            
    except KeyboardInterrupt:
        print("\nChaos Manager stopped.")

if __name__ == "__main__":
    # Ensure PID directory exists
    if not os.path.exists("pids"):
        os.makedirs("pids")
    run()