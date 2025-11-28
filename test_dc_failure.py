"""
Datacenter failure simulation and testing
Tests system behavior when one datacenter goes down
"""

import time
import argparse
import subprocess
import signal
import os

import grpc
import dht_pb2
import dht_pb2_grpc


class DatacenterFailureTest:
    """Test datacenter failure scenarios."""
    
    def __init__(self, nodes_by_dc: dict):
        """
        Initialize with nodes grouped by datacenter.
        """
        self.nodes_by_dc = nodes_by_dc
        self.all_nodes = []
        for nodes in nodes_by_dc.values():
            self.all_nodes.extend(nodes)
        
        self.stubs = {}
        self._create_stubs()
    
    def _create_stubs(self):
        """Create gRPC stubs for all nodes."""
        for node in self.all_nodes:
            try:
                channel = grpc.insecure_channel(node)
                self.stubs[node] = dht_pb2_grpc.DHTServiceStub(channel)
            except Exception as e:
                print(f"Warning: Failed to connect to {node}: {e}")
    
    def check_cluster_health(self):
        """Check health of all nodes in the cluster."""
        print(f"\n{'='*70}")
        print("CLUSTER HEALTH CHECK")
        print(f"{'='*70}")
        
        health_status = {}
        
        for dc_name, nodes in self.nodes_by_dc.items():
            print(f"\n{dc_name}:")
            dc_healthy = 0
            dc_total = len(nodes)
            
            for node in nodes:
                try:
                    stub = self.stubs.get(node)
                    if stub:
                        request = dht_pb2.HealthCheckRequest(node_id="test")
                        response = stub.HealthCheck(request, timeout=2)
                        if response.healthy:
                            print(f"  ✓ {node} - HEALTHY")
                            dc_healthy += 1
                        else:
                            print(f"  ✗ {node} - UNHEALTHY")
                    else:
                        print(f"  ✗ {node} - NO STUB")
                except Exception as e:
                    print(f"  ✗ {node} - UNREACHABLE ({str(e)[:30]})")
            
            health_status[dc_name] = (dc_healthy, dc_total)
        
        return health_status
    
    def populate_test_data(self, num_keys=20):
        """Populate cluster with test data."""
        print(f"\n{'='*70}")
        print(f"POPULATING TEST DATA ({num_keys} keys)")
        print(f"{'='*70}")
        
        successful = 0
        stub = list(self.stubs.values())[0]
        
        for i in range(num_keys):
            key = f"failtest:key{i}"
            value = f"value{i}_original"
            
            try:
                request = dht_pb2.PutRequest(key=key, value=value)
                response = stub.Put(request, timeout=5)
                
                if response.success:
                    successful += 1
                    if i % 5 == 0:
                        print(f"  Stored {i+1}/{num_keys} keys...")
            except Exception as e:
                print(f"  Error storing {key}: {e}")
        
        print(f"✓ Successfully stored {successful}/{num_keys} keys")
        time.sleep(2)
        return successful
    
    def test_reads_after_dc_failure(self, failed_dc: str, num_keys=20):
        """
        Test if reads still work after a datacenter fails.
        """
        print(f"\n{'='*70}")
        print(f"TESTING READS AFTER {failed_dc} FAILURE")
        print(f"{'='*70}")
        
        healthy_nodes = []
        for dc_name, nodes in self.nodes_by_dc.items():
            if dc_name != failed_dc:
                healthy_nodes.extend(nodes)
        
        if not healthy_nodes:
            print("✗ No healthy nodes available!")
            return 0, num_keys
        
        print(f"Using {len(healthy_nodes)} healthy nodes from other datacenters")
        
        successful = 0
        
        for i in range(num_keys):
            key = f"failtest:key{i}"
            
            for node in healthy_nodes:
                try:
                    stub = self.stubs.get(node)
                    if not stub:
                        continue
                    
                    request = dht_pb2.GetRequest(key=key)
                    response = stub.Get(request, timeout=5)
                    
                    if response.success:
                        successful += 1
                        if i % 5 == 0:
                            print(f"  ✓ Read {i+1}/{num_keys} keys successfully")
                        break
                    
                except Exception as e:
                    continue
        
        success_rate = (successful / num_keys) * 100
        print(f"\n{'='*70}")
        print(f"READ TEST RESULTS:")
        print(f"  Successful: {successful}/{num_keys} ({success_rate:.1f}%)")
        print(f"{'='*70}")
        
        return successful, num_keys
    
    def test_writes_after_dc_failure(self, failed_dc: str, num_keys=10):
        """
        Test if writes still work after a datacenter fails.
        """
        print(f"\n{'='*70}")
        print(f"TESTING WRITES AFTER {failed_dc} FAILURE")
        print(f"{'='*70}")
        
        healthy_nodes = []
        for dc_name, nodes in self.nodes_by_dc.items():
            if dc_name != failed_dc:
                healthy_nodes.extend(nodes)
        
        if not healthy_nodes:
            print("✗ No healthy nodes available!")
            return 0, num_keys
        
        successful = 0
        stub = self.stubs[healthy_nodes[0]]
        
        for i in range(num_keys):
            key = f"failtest:newkey{i}"
            value = f"newvalue{i}_after_failure"
            
            try:
                request = dht_pb2.PutRequest(key=key, value=value)
                response = stub.Put(request, timeout=5)
                
                if response.success:
                    successful += 1
                    if i % 5 == 0:
                        print(f"  ✓ Wrote {i+1}/{num_keys} keys successfully")
                        
            except Exception as e:
                print(f"  ✗ Failed to write {key}: {e}")
        
        success_rate = (successful / num_keys) * 100
        print(f"\n{'='*70}")
        print(f"WRITE TEST RESULTS:")
        print(f"  Successful: {successful}/{num_keys} ({success_rate:.1f}%)")
        print(f"{'='*70}")
        
        return successful, num_keys


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Datacenter Failure Testing')
    parser.add_argument('--dc1-nodes', type=str, nargs='+', 
                       default=['localhost:50051', 'localhost:50052'],
                       help='DC1 node addresses')
    parser.add_argument('--dc2-nodes', type=str, nargs='+',
                       default=['localhost:50053', 'localhost:50054'],
                       help='DC2 node addresses')
    parser.add_argument('--dc3-nodes', type=str, nargs='+',
                       default=['localhost:50055', 'localhost:50056'],
                       help='DC3 node addresses')
    parser.add_argument('--fail-dc', type=str, default='DC1',
                       choices=['DC1', 'DC2', 'DC3'],
                       help='Which datacenter to simulate failure for')
    
    args = parser.parse_args()
    
    nodes_by_dc = {
        'DC1': args.dc1_nodes,
        'DC2': args.dc2_nodes,
        'DC3': args.dc3_nodes
    }
    
    print("\n" + "="*70)
    print("DATACENTER FAILURE TEST")
    print("="*70)
    print(f"\nConfiguration:")
    for dc, nodes in nodes_by_dc.items():
        print(f"  {dc}: {nodes}")
    print(f"\nSimulated Failure: {args.fail_dc}")
    
    tester = DatacenterFailureTest(nodes_by_dc)
    
    print("\n[STEP 1] Initial health check")
    tester.check_cluster_health()
    
    print("\n[STEP 2] Populate test data")
    num_populated = tester.populate_test_data(num_keys=20)
    
    print(f"\n[STEP 3] Simulating {args.fail_dc} failure")
    print(f"⚠ NOTE: This test assumes nodes are already down.")
    print(f"  To simulate failure, stop nodes in {args.fail_dc} manually:")
    for node in nodes_by_dc[args.fail_dc]:
        print(f"    • {node}")
    input("\nPress Enter when nodes are stopped...")
    
    print("\n[STEP 4] Health check after failure")
    health_after = tester.check_cluster_health()
    
    print("\n[STEP 5] Test read availability")
    read_success, read_total = tester.test_reads_after_dc_failure(args.fail_dc, num_keys=20)
    
    print("\n[STEP 6] Test write availability")
    write_success, write_total = tester.test_writes_after_dc_failure(args.fail_dc, num_keys=10)
    
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    print(f"\nFailed Datacenter: {args.fail_dc}")
    print(f"\nRead Availability:  {read_success}/{read_total} ({read_success/read_total*100:.1f}%)")
    print(f"Write Availability: {write_success}/{write_total} ({write_success/write_total*100:.1f}%)")
    
    read_ok = (read_success / read_total) >= 0.9  # 90% threshold
    write_ok = (write_success / write_total) >= 0.9
    
    if read_ok and write_ok:
        print(f"\n✓ PASS: System maintains availability after DC failure")
        return 0
    else:
        print(f"\n✗ FAIL: System availability degraded below acceptable level")
        return 1


if __name__ == '__main__':
    exit(main())
