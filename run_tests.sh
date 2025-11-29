#!/bin/bash
# Quick test runner with different configurations

echo "=================================================================="
echo "DHT Test Suite Runner"
echo "=================================================================="
echo ""
echo "Select test configuration:"
echo ""
echo "1. FULL TEST (100 nodes, 10 DCs, 500 items) - ~10 minutes"
echo "2. LIGHT TEST (20 nodes, 2 DCs, 100 items) - ~3 minutes"
echo "3. QUICK SMOKE TEST (10 nodes, 1 DC, 50 items) - ~1 minute"
echo "4. CUSTOM - Run specific phases"
echo "5. CLEANUP ONLY - Remove all data and logs"
echo ""
read -p "Enter choice (1-5): " choice

case $choice in
    1)
        echo ""
        echo "Running FULL comprehensive test..."
        echo "This will take approximately 10 minutes."
        read -p "Continue? (y/n): " confirm
        if [ "$confirm" = "y" ]; then
            python3 test_comprehensive.py
        fi
        ;;
    2)
        echo ""
        echo "Running LIGHT test..."
        python3 test_light.py
        ;;
    3)
        echo ""
        echo "Running QUICK smoke test..."
        # Create a minimal test inline
        python3 -c "
import subprocess
import sys

# Modify test parameters
with open('test_comprehensive.py', 'r') as f:
    content = f.read()

content = content.replace('NUM_DATACENTERS = 10', 'NUM_DATACENTERS = 1')
content = content.replace('NODES_PER_DC = 10', 'NODES_PER_DC = 10')
content = content.replace('NUM_CLIENTS = 5', 'NUM_CLIENTS = 2')
content = content.replace('NUM_ITEMS = 500', 'NUM_ITEMS = 50')
content = content.replace('duration = 30', 'duration = 10')  # Shorter stress test

exec(content)
"
        ;;
    4)
        echo ""
        echo "Available phases:"
        echo "  1 - Cluster Deployment"
        echo "  2 - Initial Data Load"
        echo "  3 - Concurrent Client Operations"
        echo "  4 - Node Failures and Recovery"
        echo "  5 - Consistency Verification"
        echo "  6 - Load Distribution Analysis"
        echo "  7 - Persistence Verification"
        echo "  8 - Stress Test"
        echo "  9 - Datacenter Awareness Test"
        echo " 10 - Client-Side Vector Clock Tracking Test"
        echo " 11 - Read Repair Test"
        echo ""
        read -p "Enter phases to run (comma-separated, e.g., 1,2,3): " phases
        python3 test_comprehensive.py --phases "$phases"
        ;;
    5)
        echo ""
        echo "Cleaning up all data and logs..."
        pkill -f server_v2.py 2>/dev/null
        rm -rf data/*.json logs/*.log
        echo "✓ Cleanup complete"
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "=================================================================="
echo "Test runner complete"
echo "=================================================================="
