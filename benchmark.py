"""
Performance benchmarking and monitoring tool for DHT
Tests throughput, latency, and system behavior under load
"""

import time
import statistics
import argparse
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
from datetime import datetime

import grpc
import dht_pb2
import dht_pb2_grpc


class DHTBenchmark:
    """Performance benchmarking for DHT."""
    
    def __init__(self, nodes: list):
        """Initialize benchmark with node addresses."""
        self.nodes = nodes
        self.stubs = {}
        
        for node in nodes:
            try:
                channel = grpc.insecure_channel(node)
                self.stubs[node] = dht_pb2_grpc.DHTServiceStub(channel)
            except Exception as e:
                print(f"Warning: Failed to connect to {node}: {e}")
    
    def _random_string(self, length=10):
        """Generate random string."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def benchmark_put_latency(self, num_operations=100):
        """
        Benchmark PUT operation latency.
        """
        print(f"\n{'='*70}")
        print(f"BENCHMARK: PUT Latency ({num_operations} operations)")
        print(f"{'='*70}")
        
        latencies = []
        failures = 0
        
        for i in range(num_operations):
            key = f"bench:put:{i}"
            value = self._random_string(100)
            
            stub = random.choice(list(self.stubs.values()))
            
            try:
                start_time = time.time()
                request = dht_pb2.PutRequest(key=key, value=value)
                response = stub.Put(request, timeout=10)
                end_time = time.time()
                
                if response.success:
                    latency_ms = (end_time - start_time) * 1000
                    latencies.append(latency_ms)
                else:
                    failures += 1
                    
            except Exception as e:
                failures += 1
        
        if latencies:
            results = {
                'operation': 'PUT',
                'count': len(latencies),
                'failures': failures,
                'min_ms': min(latencies),
                'max_ms': max(latencies),
                'mean_ms': statistics.mean(latencies),
                'median_ms': statistics.median(latencies),
                'stdev_ms': statistics.stdev(latencies) if len(latencies) > 1 else 0,
                'latencies': latencies
            }
            
            self._print_results(results)
            return results
        else:
            print("No successful operations")
            return None
    
    def benchmark_get_latency(self, num_operations=100):
        """
        Benchmark GET operation latency.
        """
        print(f"\n{'='*70}")
        print(f"BENCHMARK: GET Latency ({num_operations} operations)")
        print(f"{'='*70}")
        
        print("Populating test data...")
        stub = random.choice(list(self.stubs.values()))
        for i in range(num_operations):
            key = f"bench:get:{i}"
            value = self._random_string(100)
            try:
                request = dht_pb2.PutRequest(key=key, value=value)
                stub.Put(request, timeout=10)
            except:
                pass
        
        time.sleep(1)
        
        latencies = []
        failures = 0
        
        for i in range(num_operations):
            key = f"bench:get:{i}"
            stub = random.choice(list(self.stubs.values()))
            
            try:
                start_time = time.time()
                request = dht_pb2.GetRequest(key=key)
                response = stub.Get(request, timeout=10)
                end_time = time.time()
                
                if response.success:
                    latency_ms = (end_time - start_time) * 1000
                    latencies.append(latency_ms)
                else:
                    failures += 1
                    
            except Exception as e:
                failures += 1
        
        if latencies:
            results = {
                'operation': 'GET',
                'count': len(latencies),
                'failures': failures,
                'min_ms': min(latencies),
                'max_ms': max(latencies),
                'mean_ms': statistics.mean(latencies),
                'median_ms': statistics.median(latencies),
                'stdev_ms': statistics.stdev(latencies) if len(latencies) > 1 else 0,
                'latencies': latencies
            }
            
            self._print_results(results)
            return results
        else:
            print("No successful operations")
            return None
    
    def benchmark_throughput(self, duration_seconds=10, num_threads=4):
        """
        Benchmark system throughput with concurrent clients.
        """
        print(f"\n{'='*70}")
        print(f"BENCHMARK: Throughput ({duration_seconds}s, {num_threads} threads)")
        print(f"{'='*70}")
        
        operations_completed = {'put': 0, 'get': 0}
        errors = 0
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        def worker():
            """Worker thread function."""
            local_ops = {'put': 0, 'get': 0}
            local_errors = 0
            
            while time.time() < end_time:
                try:
                    stub = random.choice(list(self.stubs.values()))
                    
                    if random.random() < 0.5:
                        key = f"throughput:{self._random_string(5)}"
                        value = self._random_string(50)
                        request = dht_pb2.PutRequest(key=key, value=value)
                        response = stub.Put(request, timeout=5)
                        if response.success:
                            local_ops['put'] += 1
                        else:
                            local_errors += 1
                    else:
                        key = f"throughput:{self._random_string(5)}"
                        request = dht_pb2.GetRequest(key=key)
                        response = stub.Get(request, timeout=5)
                        local_ops['get'] += 1
                        
                except Exception as e:
                    local_errors += 1
            
            return local_ops, local_errors
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker) for _ in range(num_threads)]
            
            for future in as_completed(futures):
                try:
                    ops, errs = future.result()
                    operations_completed['put'] += ops['put']
                    operations_completed['get'] += ops['get']
                    errors += errs
                except Exception as e:
                    print(f"Worker error: {e}")
        
        actual_duration = time.time() - start_time
        total_ops = operations_completed['put'] + operations_completed['get']
        
        results = {
            'duration_s': actual_duration,
            'threads': num_threads,
            'total_operations': total_ops,
            'put_operations': operations_completed['put'],
            'get_operations': operations_completed['get'],
            'errors': errors,
            'throughput_ops_sec': total_ops / actual_duration,
            'put_ops_sec': operations_completed['put'] / actual_duration,
            'get_ops_sec': operations_completed['get'] / actual_duration
        }
        
        print(f"\nResults:")
        print(f"  Duration:        {results['duration_s']:.2f}s")
        print(f"  Total Ops:       {results['total_operations']}")
        print(f"  PUT Ops:         {results['put_operations']}")
        print(f"  GET Ops:         {results['get_operations']}")
        print(f"  Errors:          {results['errors']}")
        print(f"  Throughput:      {results['throughput_ops_sec']:.1f} ops/sec")
        print(f"  PUT Rate:        {results['put_ops_sec']:.1f} ops/sec")
        print(f"  GET Rate:        {results['get_ops_sec']:.1f} ops/sec")
        
        return results
    
    def _print_results(self, results):
        """Print benchmark results."""
        print(f"\nResults:")
        print(f"  Successful:   {results['count']}")
        print(f"  Failures:     {results['failures']}")
        print(f"  Min Latency:  {results['min_ms']:.2f} ms")
        print(f"  Max Latency:  {results['max_ms']:.2f} ms")
        print(f"  Mean Latency: {results['mean_ms']:.2f} ms")
        print(f"  Median:       {results['median_ms']:.2f} ms")
        print(f"  Std Dev:      {results['stdev_ms']:.2f} ms")
    
    def plot_latency_distribution(self, results, output_file='latency_dist.png'):
        """
        Plot latency distribution histogram.
        
        Args:
            results: Results from benchmark
            output_file: Output file path
        """
        try:
            import matplotlib.pyplot as plt
            
            plt.figure(figsize=(10, 6))
            plt.hist(results['latencies'], bins=50, edgecolor='black', alpha=0.7)
            plt.xlabel('Latency (ms)')
            plt.ylabel('Frequency')
            plt.title(f"{results['operation']} Operation Latency Distribution")
            plt.axvline(results['mean_ms'], color='r', linestyle='--', 
                       label=f'Mean: {results["mean_ms"]:.2f}ms')
            plt.axvline(results['median_ms'], color='g', linestyle='--', 
                       label=f'Median: {results["median_ms"]:.2f}ms')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.savefig(output_file)
            print(f"\n✓ Latency distribution saved to {output_file}")
        except ImportError:
            print("\n⚠ matplotlib not installed, skipping plot")
        except Exception as e:
            print(f"\n⚠ Failed to create plot: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='DHT Performance Benchmark')
    parser.add_argument('--nodes', type=str, nargs='+', 
                       default=['localhost:50051'],
                       help='Node addresses')
    parser.add_argument('--put-ops', type=int, default=100,
                       help='Number of PUT operations for latency test')
    parser.add_argument('--get-ops', type=int, default=100,
                       help='Number of GET operations for latency test')
    parser.add_argument('--throughput-duration', type=int, default=10,
                       help='Throughput test duration (seconds)')
    parser.add_argument('--threads', type=int, default=4,
                       help='Number of concurrent threads')
    parser.add_argument('--plot', action='store_true',
                       help='Generate latency plots')
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("DHT PERFORMANCE BENCHMARK")
    print("="*70)
    print(f"Nodes: {args.nodes}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    benchmark = DHTBenchmark(args.nodes)
    
    put_results = benchmark.benchmark_put_latency(args.put_ops)
    get_results = benchmark.benchmark_get_latency(args.get_ops)
    throughput_results = benchmark.benchmark_throughput(
        args.throughput_duration, 
        args.threads
    )
    
    if args.plot:
        if put_results:
            benchmark.plot_latency_distribution(put_results, 'put_latency.png')
        if get_results:
            benchmark.plot_latency_distribution(get_results, 'get_latency.png')
    
    print("\n" + "="*70)
    print("BENCHMARK COMPLETED")
    print("="*70 + "\n")


if __name__ == '__main__':
    main()
