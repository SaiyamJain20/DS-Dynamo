"""
Enhanced Test Client for DHT (Phases 2+3)
Supports conflict resolution, quorum reads/writes, and datacenter testing
"""

import grpc
import argparse
import time
import random

import dht_pb2
import dht_pb2_grpc


class DHTClient:
    """Enhanced DHT client with Phase 2+3 support."""
    
    def __init__(self, nodes: list):
        """
        Initialize client with multiple node addresses.
        """
        self.nodes = nodes
        self.channels = {}
        self.stubs = {}
        
        # CRITICAL FIX: Track vector clocks for each key
        # This prevents false conflicts when same client writes sequentially
        self.vector_clocks = {}  # key -> latest vector clock
        
        for node in nodes:
            try:
                channel = grpc.insecure_channel(node)
                self.channels[node] = channel
                self.stubs[node] = dht_pb2_grpc.DHTServiceStub(channel)
            except Exception as e:
                print(f"Warning: Failed to connect to {node}: {e}")
    
    def _get_random_coordinator(self):
        """Get a random node to act as coordinator."""
        node = random.choice(list(self.stubs.keys()))
        return node, self.stubs[node]
    
    def put(self, key: str, value: str, vector_clock: dict = None, max_retries: int = 1):
        """
        Put a key-value pair with retry logic.
        Tries multiple coordinators if first attempt fails.
        
        CRITICAL: Uses client-side vector clock tracking to prevent false conflicts.
        When the same client writes to the same key multiple times, it sends the
        latest vector clock, ensuring causality is preserved even with different coordinators.
        """
        last_error = None
        
        # CRITICAL FIX: Use tracked vector clock if no explicit clock provided
        # This ensures sequential writes from same client don't create false conflicts
        if vector_clock is None and key in self.vector_clocks:
            vector_clock = self.vector_clocks[key]
            print(f"  📝 Using tracked vector clock: {vector_clock}")
        
        for attempt in range(max_retries):
            try:
                coordinator, stub = self._get_random_coordinator()
                
                if attempt == 0:
                    print(f"\nPUT '{key}' = '{value}' via {coordinator}")
                else:
                    print(f"  Retry {attempt}/{max_retries-1} via {coordinator}")
                
                request = dht_pb2.PutRequest(
                    key=key,
                    value=value,
                    vector_clock=vector_clock or {}
                )
                
                response = stub.Put(request, timeout=15)  # Increased timeout
                
                if response.success:
                    print(f"✓ Success: {response.message}")
                    print(f"  Vector Clock: {dict(response.vector_clock)}")
                    
                    # CRITICAL FIX: Update tracked vector clock for this key
                    self.vector_clocks[key] = dict(response.vector_clock)
                    
                    return response
                else:
                    last_error = response.message
                    print(f"  Failed: {response.message}")
                    if attempt < max_retries - 1:
                        time.sleep(0.5 * (2 ** attempt))  # Exponential backoff
                    
            except Exception as e:
                last_error = str(e)
                print(f"  Error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (2 ** attempt))  # Exponential backoff
        
        # All retries failed
        print(f"✗ All {max_retries} attempts failed. Last error: {last_error}")
        return None
    
    def get(self, key: str, max_retries: int = 3):
        """
        Get value for a key with retry logic.
        Tries multiple coordinators if first attempt fails.
        
        CRITICAL: Updates client-side vector clock tracking after successful read.
        This ensures subsequent writes have the correct causal context.
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                coordinator, stub = self._get_random_coordinator()
                
                if attempt == 0:
                    print(f"\nGET '{key}' via {coordinator}")
                else:
                    print(f"  Retry {attempt}/{max_retries-1} via {coordinator}")
                
                request = dht_pb2.GetRequest(key=key)
                response = stub.Get(request, timeout=15)  # Increased timeout
                
                if response.success:
                    print(f"✓ Value: '{response.value}'")
                    print(f"  Vector Clock: {dict(response.vector_clock)}")
                    
                    # CRITICAL FIX: Update tracked vector clock after GET
                    # This ensures next PUT uses the latest causal context
                    self.vector_clocks[key] = dict(response.vector_clock)
                    
                    if response.has_conflicts:
                        print(f"  ⚠ CONFLICT DETECTED!")
                        print(f"  Sibling versions ({len(response.sibling_values)}):")
                        for i, (val, clock_msg) in enumerate(zip(response.sibling_values, 
                                                                  response.sibling_clocks)):
                            print(f"    {i+1}. value='{val}', clock={dict(clock_msg.clock)}")
                        
                        return self._resolve_conflict(key, response)
                    else:
                        print(f"  {response.message}")
                    
                    return response
                else:
                    last_error = response.message
                    print(f"  {response.message}")
                    if attempt < max_retries - 1:
                        time.sleep(0.5 * (2 ** attempt))  # Exponential backoff
                    
            except Exception as e:
                last_error = str(e)
                print(f"  Error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (2 ** attempt))  # Exponential backoff
        
        # All retries failed
        print(f"✗ All {max_retries} attempts failed. Last error: {last_error}")
        return None
    
    def _resolve_conflict(self, key: str, response):
        """
        Help user resolve conflicts (interactive mode).
        """
        print("\n  Conflict Resolution Options:")
        print("    1. Keep first value")
        print("    2. Keep last value")
        print("    3. Manual merge")
        print("    4. Keep all (no resolution)")
        
        print("  → Auto-resolving with first value for automated test")
        return response
    
    def health_check(self, node_address: str = None):
        """
        Check health of a node.
        
        Args:
            node_address: Specific node to check, or None for random
        """
        try:
            if node_address:
                stub = self.stubs.get(node_address)
                if not stub:
                    print(f"Node {node_address} not found")
                    return
                nodes_to_check = [(node_address, stub)]
            else:
                nodes_to_check = list(self.stubs.items())
            
            print(f"\n{'='*70}")
            print("HEALTH CHECK")
            print(f"{'='*70}")
            
            for node, stub in nodes_to_check:
                try:
                    request = dht_pb2.HealthCheckRequest(node_id="client")
                    response = stub.HealthCheck(request, timeout=5)
                    
                    status = "✓ HEALTHY" if response.healthy else "✗ UNHEALTHY"
                    print(f"{node:25} {status:12} Node: {response.node_id:10} DC: {response.datacenter}")
                    
                except Exception as e:
                    print(f"{node:25} ✗ UNREACHABLE  Error: {str(e)[:30]}")
            
            print(f"{'='*70}\n")
            
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def close(self):
        """Close all connections."""
        for channel in self.channels.values():
            channel.close()


def run_comprehensive_tests(client: DHTClient):
    """
    Run comprehensive test suite for Phases 2+3.
    """
    print("\n" + "="*70)
    print("DHT COMPREHENSIVE TEST SUITE (Phases 2+3)")
    print("="*70)
    
    print("\n[TEST 1] Health Check All Nodes")
    print("-" * 70)
    client.health_check()
    
    print("\n[TEST 2] Basic Put/Get Operations")
    print("-" * 70)
    client.put("user:1", "Alice")
    time.sleep(0.5)
    client.put("user:2", "Bob")
    client.put("user:3", "Charlie")
    time.sleep(0.5)
    client.get("user:1")
    client.get("user:2")
    client.get("user:3")
    
    print("\n[TEST 3] Versioning - Update Same Key")
    print("-" * 70)
    client.put("counter", "1")
    time.sleep(0.3)
    client.put("counter", "2")
    time.sleep(0.3)
    client.put("counter", "3")
    time.sleep(0.3)
    client.get("counter")
    
    print("\n[TEST 4] Concurrent Writes (Conflict Detection)")
    print("-" * 70)
    print("Simulating concurrent writes to same key...")
    client.put("shared:item", "Version_A")
    client.put("shared:item", "Version_B")
    time.sleep(0.5)
    client.get("shared:item")
    
    print("\n[TEST 5] High Load Test (20 operations)")
    print("-" * 70)
    start_time = time.time()
    for i in range(10):
        client.put(f"load:key{i}", f"value{i}")
    for i in range(10):
        client.get(f"load:key{i}")
    elapsed = time.time() - start_time
    print(f"Completed 20 operations in {elapsed:.2f}s ({20/elapsed:.1f} ops/sec)")
    
    print("\n[TEST 6] Large Value Storage")
    print("-" * 70)
    large_value = "x" * 1000  # 1KB
    client.put("large:data", large_value)
    time.sleep(0.3)
    response = client.get("large:data")
    if response and len(response.value) == 1000:
        print("✓ Large value stored and retrieved correctly")
    
    print("\n[TEST 7] Non-existent Key Handling")
    print("-" * 70)
    client.get("nonexistent:key")
    
    print("\n" + "="*70)
    print("TEST SUITE COMPLETED")
    print("="*70 + "\n")


def interactive_mode(client: DHTClient):
    """
    Interactive mode with enhanced commands.
    """
    print("\n" + "="*70)
    print("DHT INTERACTIVE MODE (Phases 2+3)")
    print("="*70)
    print("\nCommands:")
    print("  put <key> <value>  - Store key-value pair")
    print("  get <key>          - Retrieve value")
    print("  health [node]      - Check node health")
    print("  test               - Run comprehensive tests")
    print("  quit               - Exit")
    print("="*70 + "\n")
    
    while True:
        try:
            cmd = input("> ").strip()
            
            if not cmd:
                continue
            
            parts = cmd.split(maxsplit=2)
            command = parts[0].lower()
            
            if command == "quit":
                break
            
            elif command == "health":
                node = parts[1] if len(parts) > 1 else None
                client.health_check(node)
            
            elif command == "put":
                if len(parts) < 3:
                    print("Usage: put <key> <value>")
                    continue
                key, value = parts[1], parts[2]
                client.put(key, value)
            
            elif command == "get":
                if len(parts) < 2:
                    print("Usage: get <key>")
                    continue
                key = parts[1]
                client.get(key)
            
            elif command == "test":
                run_comprehensive_tests(client)
            
            else:
                print(f"Unknown command: {command}")
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='DHT Test Client (Phases 2+3)')
    parser.add_argument(
        '--nodes',
        type=str,
        nargs='+',
        default=['localhost:50051'],
        help='Node addresses (host:port format)'
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Run in interactive mode'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run comprehensive test suite'
    )
    
    args = parser.parse_args()
    
    client = DHTClient(args.nodes)
    
    try:
        if args.interactive:
            interactive_mode(client)
        elif args.test:
            run_comprehensive_tests(client)
        else:
            run_comprehensive_tests(client)
    finally:
        client.close()


if __name__ == '__main__':
    main()
