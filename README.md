# DS-Dynamo: Distributed Hash Table Implementation

A Python implementation of Amazon Dynamo's distributed hash table with support for replication, eventual consistency, datacenter awareness, and vector clocks.

## Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

## Installation

1. **Clone the repository**:
```bash
git clone https://github.com/SaiyamJain20/DS-Dynamo.git
cd DS-Dynamo
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **Generate gRPC code** (if needed):
```bash
./generate_grpc.sh
```

## Usage

### Running a Single Node

Start a single DHT node:
```bash
python server_v2.py --node-id node1 --port 50051 --datacenter DC1
```

### Running a Cluster

#### Option 1: Using the deployment script
```bash
./deploy_cluster.sh
```
This starts a 6-node cluster (2 datacenters, 3 nodes each) on ports 50051-50056.

#### Option 2: Manual cluster setup
```bash
python server_v2.py --node-id dc1_node1 --port 50051 --datacenter DC1
python server_v2.py --node-id dc1_node2 --port 50052 --datacenter DC1 --known-nodes localhost:50051
python server_v2.py --node-id dc2_node1 --port 50053 --datacenter DC2 --known-nodes localhost:50051
```

### Using the Client

#### Interactive Mode
```bash
python client_v2.py --nodes localhost:50051 localhost:50052 localhost:50053 --interactive
```

Commands:
- `put <key> <value>` - Store a key-value pair
- `get <key>` - Retrieve value for a key
- `delete <key>` - Delete a key
- `exit` - Exit the client

### Running Tests

#### Comprehensive Test Suite (All 11 Phases)
```bash
python test_comprehensive.py
```

This runs all test phases:
1. Cluster deployment
2. Initial data load
3. Concurrent client operations
4. Node failures and recovery
5. Consistency verification
6. Load distribution analysis
7. Persistence verification
8. Stress testing
9. Datacenter awareness
10. Vector clock tracking
11. Read repair

#### Run Specific Test Phases
```bash
# Run only phases 10 and 11 against existing cluster
python test_comprehensive.py --phases 10,11 --skip-cleanup

# Run phases 1-5
python test_comprehensive.py --phases 1,2,3,4,5
```

#### Quick Test Script
```bash
./run_tests.sh
```

## Configuration

Edit `config.py` to customize:

```python
class NodeConfig:
    # Replication parameters
    N = 3  # Number of replicas
    R = 2  # Read quorum
    W = 2  # Write quorum
    
    # Consistent hashing
    NUM_VIRTUAL_NODES = 150  # Virtual nodes per physical node
    
    # Read repair
    ENABLE_READ_REPAIR = True
    READ_REPAIR_ASYNC = True
    
    # Hinted handoff
    ENABLE_HINTED_HANDOFF = True
    HINT_DELIVERY_INTERVAL_SEC = 30
    HINT_MAX_AGE_SEC = 600
    HINT_MAX_ATTEMPTS = 100
    SLOPPY_QUORUM_EXTRA_NODES = 2
    
    # Datacenter awareness
    DC_REPLICATION_STRATEGY = "CROSS_DC"
    MIN_DATACENTERS = 2
```

## Troubleshooting

### Ring Convergence Issues
If nodes don't see the full cluster:
```bash
# Clean restart all nodes
./CLEAN_RESTART.sh
```

### Data Cleanup
Remove all data and restart fresh:
```bash
rm -rf data/
./deploy_cluster.sh
```

### Port Already in Use
```bash
# Check running processes
lsof -i :50051

# Kill processes if needed
pkill -f server_v2.py
```

## Graphs Generator
Run the bash script
```bash
chmod +x graph_generator.sh
./graph_generator.sh
```

## Website
Run the bash script for automated testing of website
```bash
chmod +x run_demo_with_chaos.sh
./run_demo_with_chaos.sh
```

If want to run only the website
```bash
python3 dashboard.py
```

## Authors

- Shail Shah
- Chaitanya Shah
- Saiyam Jain

## Course Information

Semester 5 - Distributed Systems  
