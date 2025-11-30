import subprocess
import time
import sys
import os

BASE_PORT = 50051
NUM_NODES = 10
NUM_DCS = 3

print(f"--- Starting Demo Cluster ({NUM_NODES} nodes) ---")

topology = []
for i in range(NUM_NODES):
    dc = f"DC{(i % NUM_DCS) + 1}"
    topology.append(dc)

# Ensure logs/pids directory exists
os.makedirs("logs", exist_ok=True)
os.makedirs("pids", exist_ok=True) # New directory for PID files

def start_node(i, is_seed=False):
    port = BASE_PORT + i
    node_id = f"node_{i}" if not is_seed else "node_0"
    
    cmd = [
        sys.executable, "server_v2.py",
        "--node-id", node_id,
        "--host", "localhost",
        "--port", str(port),
        "--datacenter", topology[i],
        "--n", "3", "--r", "2", "--w", "2",
        "--enable-dc-aware"
    ]
    
    if not is_seed:
        cmd.extend(["--known-nodes", f"localhost:{BASE_PORT}"])
        
    print(f"Starting {node_id}: localhost:{port} ({topology[i]})")
    log_file = open(f"logs/{node_id}.log", "a")
    p = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
    
    # Save PID for Chaos Monkey
    with open(f"pids/{node_id}.pid", "w") as f:
        f.write(str(p.pid))
        
    return p

# Start Seed
start_node(0, is_seed=True)
time.sleep(2)

# Start Others
for i in range(1, NUM_NODES):
    start_node(i)
    time.sleep(0.5)

print("\n--- Cluster Ready! ---")
print("PIDs saved to pids/ directory.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping...")