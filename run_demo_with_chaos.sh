#!/bin/bash

cleanup() {
    echo ""
    echo "--- SHUTTING DOWN ---"
    pkill -f server_v2.py
    pkill -f dashboard.py
    pkill -f traffic_generator.py
    pkill -f chaos_monkey.py
    exit
}

trap cleanup SIGINT

echo "==================================================="
echo "      DHT CHAOS DEMO (With Visualizer)"
echo "==================================================="

# 1. Cleanup
echo "[1/5] Cleaning old processes..."
pkill -f server_v2.py
pkill -f dashboard.py
pkill -f traffic_generator.py
pkill -f chaos_monkey.py
rm -rf data/*.json logs/*.log pids/*.pid
mkdir -p data logs pids

# 2. Regenerate Code
echo "[2/5] Regenerating gRPC stubs..."
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dht.proto

# 3. Start Cluster
echo "[3/5] Starting 10-Node Cluster..."
python3 start_demo.py > logs/cluster_startup.log 2>&1 &
sleep 10

# 4. Start Dashboard
echo "[4/5] Starting Dashboard..."
python3 dashboard.py --n 3 > logs/dashboard.log 2>&1 &
echo "      Dashboard: http://localhost:9000"

# 5. Start Traffic
echo "[5/5] Starting Traffic Generator..."
python3 traffic_generator.py > logs/traffic.log 2>&1 &

echo ""
echo "==================================================="
echo " SYSTEM STABLE - STARTING CHAOS MONKEY IN 5s"
echo " Watch the Dashboard!"
echo "==================================================="
echo ""
sleep 5

# 6. Start Chaos Monkey
python3 chaos_monkey.py