#!/bin/bash

# Deployment script for 3-datacenter DHT cluster
# Supports running on 3 physical laptops or local simulation

echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║        DHT 3-DATACENTER DEPLOYMENT SCRIPT                                ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

# Configuration
MODE=${1:-"local"}  # local | distributed
DC1_HOST=${DC1_HOST:-"localhost"}
DC2_HOST=${DC2_HOST:-"localhost"}
DC3_HOST=${DC3_HOST:-"localhost"}

# Ports
DC1_PORTS="50051 50052"
DC2_PORTS="50053 50054"
DC3_PORTS="50055 50056"

# Check mode
if [ "$MODE" == "local" ]; then
    echo "Mode: LOCAL SIMULATION (all nodes on localhost)"
    echo ""
elif [ "$MODE" == "distributed" ]; then
    echo "Mode: DISTRIBUTED (nodes across 3 physical machines)"
    echo "DC1 Host: $DC1_HOST"
    echo "DC2 Host: $DC2_HOST"
    echo "DC3 Host: $DC3_HOST"
    echo ""
else
    echo "Usage: $0 [local|distributed]"
    exit 1
fi

# Generate all node addresses
ALL_NODES=""
for port in $DC1_PORTS; do
    ALL_NODES="$ALL_NODES $DC1_HOST:$port"
done
for port in $DC2_PORTS; do
    ALL_NODES="$ALL_NODES $DC2_HOST:$port"
done
for port in $DC3_PORTS; do
    ALL_NODES="$ALL_NODES $DC3_HOST:$port"
done

echo "Starting DHT Cluster with Datacenter Awareness..."
echo "Nodes: $ALL_NODES"
echo ""

# Function to start a node
start_node() {
    local node_id=$1
    local host=$2
    local port=$3
    local dc=$4
    
    echo "Starting $node_id on $host:$port (Datacenter: $dc)"
    
    if [ "$MODE" == "local" ]; then
        # Local mode - start in background
        python3 server_v2.py \
            --node-id $node_id \
            --host $host \
            --port $port \
            --datacenter $dc \
            --enable-dc-aware \
            --known-nodes $ALL_NODES \
            > logs/${node_id}.log 2>&1 &
        
        echo "  PID: $!"
    else
        # Distributed mode - SSH to remote machine
        if [ "$host" == "localhost" ]; then
            python3 server_v2.py \
                --node-id $node_id \
                --host $host \
                --port $port \
                --datacenter $dc \
                --enable-dc-aware \
                --known-nodes $ALL_NODES \
                > logs/${node_id}.log 2>&1 &
        else
            ssh $host "cd $(pwd) && python3 server_v2.py \
                --node-id $node_id \
                --host $host \
                --port $port \
                --datacenter $dc \
                --enable-dc-aware \
                --known-nodes $ALL_NODES \
                > logs/${node_id}.log 2>&1 &"
        fi
    fi
    
    sleep 1
}

# Create logs directory
mkdir -p logs

# Start DC1 nodes
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "DATACENTER 1 (DC1)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
i=1
for port in $DC1_PORTS; do
    start_node "dc1_node$i" "$DC1_HOST" "$port" "DC1"
    i=$((i+1))
done

# Start DC2 nodes
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "DATACENTER 2 (DC2)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
i=1
for port in $DC2_PORTS; do
    start_node "dc2_node$i" "$DC2_HOST" "$port" "DC2"
    i=$((i+1))
done

# Start DC3 nodes
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "DATACENTER 3 (DC3)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
i=1
for port in $DC3_PORTS; do
    start_node "dc3_node$i" "$DC3_HOST" "$port" "DC3"
    i=$((i+1))
done

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════╗"
echo "║                    CLUSTER STARTED SUCCESSFULLY                          ║"
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Cluster Configuration:"
echo "  • Total Nodes: 6 (2 per datacenter)"
echo "  • Datacenters: DC1, DC2, DC3"
echo "  • Replication: N=3 (datacenter-aware)"
echo "  • Consistency: R=2, W=2 (strong consistency)"
echo ""
echo "Test the cluster:"
echo "  python3 client_v2.py --nodes $ALL_NODES --test"
echo ""
echo "Stop the cluster:"
echo "  pkill -f server_v2.py"
echo ""
echo "View logs:"
echo "  tail -f logs/*.log"
echo ""
