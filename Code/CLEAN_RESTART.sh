#!/bin/bash
# Complete cleanup and restart script

echo "🧹 Cleaning up old cluster..."

# Kill all server processes
echo "  → Killing server processes..."
pkill -9 -f server_v2.py
sleep 2

# Remove ALL data files (including rings from old tests)
echo "  → Removing all data files..."
rm -rf data/
mkdir -p data

# Remove old log files
echo "  → Cleaning log files..."
rm -rf logs/*.log

echo ""
echo "✓ Cleanup complete!"
echo ""
echo "🚀 Starting fresh 6-node cluster..."
echo ""

# Start 6 nodes (2 per datacenter)
python3 server_v2.py --node-id dc1_node1 --host localhost --port 50051 --datacenter DC1 --enable-dc-aware \
  --known-nodes localhost:50051 localhost:50052 localhost:50053 localhost:50054 localhost:50055 localhost:50056 > logs/dc1_node1.log 2>&1 &

python3 server_v2.py --node-id dc1_node2 --host localhost --port 50052 --datacenter DC1 --enable-dc-aware \
  --known-nodes localhost:50051 localhost:50052 localhost:50053 localhost:50054 localhost:50055 localhost:50056 > logs/dc1_node2.log 2>&1 &

python3 server_v2.py --node-id dc2_node1 --host localhost --port 50053 --datacenter DC2 --enable-dc-aware \
  --known-nodes localhost:50051 localhost:50052 localhost:50053 localhost:50054 localhost:50055 localhost:50056 > logs/dc2_node1.log 2>&1 &

python3 server_v2.py --node-id dc2_node2 --host localhost --port 50054 --datacenter DC2 --enable-dc-aware \
  --known-nodes localhost:50051 localhost:50052 localhost:50053 localhost:50054 localhost:50055 localhost:50056 > logs/dc2_node2.log 2>&1 &

python3 server_v2.py --node-id dc3_node1 --host localhost --port 50055 --datacenter DC3 --enable-dc-aware \
  --known-nodes localhost:50051 localhost:50052 localhost:50053 localhost:50054 localhost:50055 localhost:50056 > logs/dc3_node1.log 2>&1 &

python3 server_v2.py --node-id dc3_node2 --host localhost --port 50056 --datacenter DC3 --enable-dc-aware \
  --known-nodes localhost:50051 localhost:50052 localhost:50053 localhost:50054 localhost:50055 localhost:50056 > logs/dc3_node2.log 2>&1 &

sleep 3

echo "✓ Nodes started!"
echo ""
echo "⏳ Waiting for ring convergence (10 seconds)..."
sleep 10

echo ""
echo "📊 Checking ring convergence..."
python3 -c "
import json
import os

if os.path.exists('data/dc1_node1_ring.json'):
    data = json.load(open('data/dc1_node1_ring.json'))
    nodes = data['nodes']
    print(f'  dc1_node1 sees {len(nodes)} nodes: {nodes}')
else:
    print('  Warning: Ring file not created yet')
"

echo ""
echo "✅ Cluster ready!"
echo ""
echo "Test with: python3 client_v2.py --nodes localhost:50051 localhost:50052 localhost:50053 localhost:50054 localhost:50055 localhost:50056 --test"
