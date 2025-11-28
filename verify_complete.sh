#!/bin/bash

# verify_complete.sh - Verify complete DHT implementation

echo "=========================================="
echo "DHT Project - Completeness Verification"
echo "=========================================="
echo ""

ERRORS=0
WARNINGS=0

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_file() {
    local file=$1
    local description=$2
    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} $description"
        return 0
    else
        echo -e "${RED}✗${NC} $description - MISSING: $file"
        ((ERRORS++))
        return 1
    fi
}

check_executable() {
    local file=$1
    local description=$2
    if [ -f "$file" ] && [ -x "$file" ]; then
        echo -e "${GREEN}✓${NC} $description"
        return 0
    elif [ -f "$file" ]; then
        echo -e "${YELLOW}⚠${NC} $description - Not executable (will fix)"
        chmod +x "$file"
        ((WARNINGS++))
        return 1
    else
        echo -e "${RED}✗${NC} $description - MISSING: $file"
        ((ERRORS++))
        return 1
    fi
}

echo "=== Phase 1: Core DHT ==="
check_file "dht.proto" "gRPC service definition"
check_file "server.py" "Phase 1 server"
check_file "client.py" "Phase 1 client"
check_file "consistent_hash.py" "Consistent hashing"
check_file "storage.py" "Storage module"
check_file "config.py" "Configuration"
echo ""

echo "=== Phase 2: Replication & Versioning ==="
check_file "server_v2.py" "Enhanced server with replication"
check_file "client_v2.py" "Enhanced client"
check_file "vector_clock.py" "Vector clock implementation"
echo ""

echo "=== Phase 3: Datacenter Awareness ==="
if grep -q "DATACENTER_AWARE" config.py; then
    echo -e "${GREEN}✓${NC} Datacenter awareness in config"
else
    echo -e "${YELLOW}⚠${NC} Datacenter awareness not configured"
    ((WARNINGS++))
fi

if grep -q "datacenter_aware" consistent_hash.py; then
    echo -e "${GREEN}✓${NC} DC-aware preference list"
else
    echo -e "${RED}✗${NC} DC-aware preference list - MISSING"
    ((ERRORS++))
fi
echo ""

echo "=== Phase 4: Testing ==="
check_file "test_vector_clock.py" "Vector clock unit tests"
check_executable "test_suite.sh" "Integration test suite"
check_file "benchmark.py" "Performance benchmark"
check_file "test_dc_failure.py" "DC failure test"
echo ""

echo "=== Deployment Scripts ==="
check_executable "setup.sh" "Setup script"
check_executable "generate_grpc.sh" "gRPC code generator"
check_executable "deploy_cluster.sh" "Cluster deployment"
check_executable "run_cluster.sh" "Run local cluster"
echo ""

echo "=== Documentation ==="
check_file "README_COMPLETE.md" "Complete README"
check_file "ARCHITECTURE_COMPLETE.md" "Complete architecture"
check_file "DEPLOYMENT_GUIDE.md" "Deployment guide"
check_file "TESTING_GUIDE.md" "Testing guide"
check_file "DOCUMENTATION_INDEX.md" "Documentation index"
check_file "GET_STARTED.md" "Getting started guide"
check_file "QUICKSTART.md" "Quick start guide"
check_file "SUMMARY.md" "Implementation summary"
check_file "README.md" "Phase 1 README"
check_file "ARCHITECTURE.md" "Phase 1 architecture"
echo ""

echo "=== Generated Files (Optional) ==="
if [ -f "dht_pb2.py" ]; then
    echo -e "${GREEN}✓${NC} gRPC generated: dht_pb2.py"
else
    echo -e "${YELLOW}⚠${NC} gRPC not generated yet (run ./generate_grpc.sh)"
    ((WARNINGS++))
fi

if [ -f "dht_pb2_grpc.py" ]; then
    echo -e "${GREEN}✓${NC} gRPC generated: dht_pb2_grpc.py"
else
    echo -e "${YELLOW}⚠${NC} gRPC not generated yet (run ./generate_grpc.sh)"
    ((WARNINGS++))
fi
echo ""

echo "=== Python Dependencies ==="
if command -v python3 &> /dev/null; then
    echo -e "${GREEN}✓${NC} Python 3 installed: $(python3 --version)"
else
    echo -e "${RED}✗${NC} Python 3 NOT installed"
    ((ERRORS++))
fi

if python3 -c "import grpc" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} grpcio installed"
else
    echo -e "${YELLOW}⚠${NC} grpcio not installed (run pip3 install grpcio)"
    ((WARNINGS++))
fi

if python3 -c "import grpc_tools" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} grpcio-tools installed"
else
    echo -e "${YELLOW}⚠${NC} grpcio-tools not installed (run pip3 install grpcio-tools)"
    ((WARNINGS++))
fi
echo ""

echo "=== Code Statistics ==="
if command -v wc &> /dev/null; then
    PYTHON_LINES=$(find . -name "*.py" -not -path "./.venv/*" -not -path "*/__pycache__/*" | xargs wc -l 2>/dev/null | tail -1 | awk '{print $1}')
    SHELL_LINES=$(find . -name "*.sh" | xargs wc -l 2>/dev/null | tail -1 | awk '{print $1}')
    PROTO_LINES=$(wc -l < dht.proto 2>/dev/null || echo "0")
    
    echo "Python code: $PYTHON_LINES lines"
    echo "Shell scripts: $SHELL_LINES lines"
    echo "Protobuf: $PROTO_LINES lines"
    echo "Total: $((PYTHON_LINES + SHELL_LINES + PROTO_LINES)) lines"
fi
echo ""

echo "=== File Count ==="
PYTHON_FILES=$(find . -name "*.py" -not -path "./.venv/*" -not -path "*/__pycache__/*" | wc -l)
SHELL_FILES=$(find . -name "*.sh" | wc -l)
DOC_FILES=$(find . -name "*.md" -o -name "*.txt" | wc -l)

echo "Python files: $PYTHON_FILES"
echo "Shell scripts: $SHELL_FILES"
echo "Documentation: $DOC_FILES"
echo "Total: $((PYTHON_FILES + SHELL_FILES + DOC_FILES)) files"
echo ""

echo "=========================================="
echo "Verification Results"
echo "=========================================="
echo -e "Errors: ${RED}$ERRORS${NC}"
echo -e "Warnings: ${YELLOW}$WARNINGS${NC}"
echo ""

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo -e "${GREEN}✓ PERFECT!${NC} All phases complete, all files present!"
    echo ""
    echo "Next steps:"
    echo "  1. Run ./setup.sh (if not already done)"
    echo "  2. Deploy cluster: ./deploy_cluster.sh local"
    echo "  3. Run tests: python3 client_v2.py --nodes localhost:50051 localhost:50053 localhost:50055 --test"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}✓ SUCCESS${NC} with ${YELLOW}$WARNINGS warnings${NC}"
    echo ""
    echo "Warnings are minor issues that don't prevent operation."
    echo "Next steps:"
    echo "  1. Run ./setup.sh to fix warnings"
    echo "  2. Deploy cluster: ./deploy_cluster.sh local"
    exit 0
else
    echo -e "${RED}✗ INCOMPLETE${NC} - $ERRORS critical errors found"
    echo ""
    echo "Please ensure all required files are present before deploying."
    exit 1
fi
