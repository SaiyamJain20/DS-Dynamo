"""
DHT Dashboard - Visualizer & Monitor
====================================
1. Cluster Monitor: Table view of all nodes and their data.
2. Ring Visualizer: Graphical representation of the Consistent Hash Ring.
"""

import sys
import time
import threading
import uvicorn
import webbrowser
import grpc
import argparse
import random
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from typing import List, Dict

import dht_pb2
import dht_pb2_grpc

# --- NEW IMPORTS ---
from consistent_hash import ConsistentHash
from config import NodeConfig

# Defaults (can be overridden by CLI)
DASHBOARD_PORT = 9000
SEED_NODE = "localhost:50051"
REPLICATION_N = 3

app = FastAPI(title="DHT Dashboard")

# ============================================================================
# 1. HTML TEMPLATE
# ============================================================================

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>DHT Cluster Visualizer</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        :root { --bg: #0f172a; --card: #1e293b; --accent: #38bdf8; }
        body { background: var(--bg); color: #e2e8f0; font-family: 'Segoe UI', sans-serif; }
        .nav-btn { cursor: pointer; padding: 10px 20px; opacity: 0.7; border-bottom: 2px solid transparent; }
        .nav-btn.active { opacity: 1; border-color: var(--accent); color: var(--accent); font-weight: bold; }
        .canvas-container { display: flex; justify-content: center; padding: 20px; }
        #ringCanvas { background: radial-gradient(circle, #1e293b 0%, #0f172a 70%); border-radius: 50%; box-shadow: 0 0 50px rgba(0,0,0,0.5); }
        /* Scrollbar */
        ::-webkit-scrollbar { width: 8px; }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
    </style>
</head>
<body class="h-screen flex flex-col">

    <!-- Header -->
    <div class="bg-slate-800 border-b border-slate-700 px-6 py-3 flex justify-between items-center shadow-lg z-10">
        <div class="flex items-center gap-4">
            <h1 class="text-xl font-bold text-sky-400">DHT<span class="text-white">Visualizer</span></h1>
            <div class="flex gap-2">
                <div class="nav-btn active" onclick="switchTab('monitor')">Cluster Data</div>
                <div class="nav-btn" onclick="switchTab('ring')">Ring Topology</div>
            </div>
        </div>
        <div class="text-sm text-slate-400">
            Seed: <span class="text-slate-200 font-mono">{{ seed_node }}</span>
            <button onclick="refreshData()" class="ml-4 bg-sky-600 hover:bg-sky-500 text-white px-3 py-1 rounded text-xs font-bold transition">REFRESH</button>
        </div>
    </div>

    <!-- TAB 1: CLUSTER MONITOR -->
    <div id="tab-monitor" class="p-6 overflow-auto flex-1">
        <!-- Stats -->
        <div class="grid grid-cols-4 gap-4 mb-6">
            <div class="bg-slate-800 p-4 rounded-lg border border-slate-700">
                <div class="text-slate-400 text-xs uppercase tracking-wider">Total Nodes</div>
                <div class="text-2xl font-bold text-white" id="stat-nodes">--</div>
            </div>
            <div class="bg-slate-800 p-4 rounded-lg border border-slate-700">
                <div class="text-slate-400 text-xs uppercase tracking-wider">Datacenters</div>
                <div class="text-2xl font-bold text-emerald-400" id="stat-dcs">--</div>
            </div>
            <div class="bg-slate-800 p-4 rounded-lg border border-slate-700">
                <div class="text-slate-400 text-xs uppercase tracking-wider">Total Keys</div>
                <div class="text-2xl font-bold text-amber-400" id="stat-keys">--</div>
            </div>
            <div class="bg-slate-800 p-4 rounded-lg border border-slate-700">
                <div class="text-slate-400 text-xs uppercase tracking-wider">Replication Factor</div>
                <div class="text-2xl font-bold text-purple-400" id="stat-n">N={{ replication_n }}</div>
            </div>
        </div>

        <!-- Node Grid -->
        <div id="node-grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
            <div class="text-center col-span-full py-10 text-slate-500 animate-pulse">Scanning Cluster via gRPC...</div>
        </div>
    </div>

    <!-- TAB 2: RING VISUALIZER -->
    <div id="tab-ring" class="hidden flex-1 relative overflow-hidden bg-slate-900">
        <div class="absolute top-4 left-4 z-10 bg-slate-800/80 backdrop-blur p-4 rounded border border-slate-700">
            <h3 class="font-bold text-sky-400 mb-2">Datacenter Legend</h3>
            <div id="ring-legend" class="space-y-1"></div>
            <div class="mt-3 text-xs text-slate-400 border-t border-slate-600 pt-2">
                Outer dots = Virtual Nodes<br>
                Colors = Datacenters
            </div>
        </div>
        <div class="canvas-container h-full items-center">
            <canvas id="ringCanvas" width="800" height="800"></canvas>
        </div>
    </div>

    <script>
        let clusterData = {};
        const dcColors = [
            '#ef4444', '#3b82f6', '#22c55e', '#eab308', '#a855f7', 
            '#ec4899', '#f97316', '#14b8a6', '#6366f1', '#84cc16'
        ];
        let dcColorMap = {};

        function switchTab(tab) {
            document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
            event.target.classList.add('active');
            
            document.getElementById('tab-monitor').classList.add('hidden');
            document.getElementById('tab-ring').classList.add('hidden');
            document.getElementById('tab-' + tab).classList.remove('hidden');
            
            if(tab === 'ring') drawRing();
        }

        async function refreshData() {
            try {
                const res = await fetch('/api/cluster-state');
                clusterData = await res.json();
                updateDCMap();
                renderMonitor();
                if(!document.getElementById('tab-ring').classList.contains('hidden')) {
                    drawRing();
                }
            } catch(e) {
                console.error("Failed to fetch", e);
            }
        }

        function updateDCMap() {
            const dcs = Array.from(new Set(clusterData.nodes.map(n => n.datacenter))).sort();
            dcColorMap = {};
            let legendHtml = '';
            dcs.forEach((dc, idx) => {
                const color = dcColors[idx % dcColors.length];
                dcColorMap[dc] = color;
                legendHtml += `
                    <div class="flex items-center gap-2 text-sm">
                        <div class="w-3 h-3 rounded-full" style="background-color: ${color}"></div> 
                        ${dc}
                    </div>`;
            });
            document.getElementById('ring-legend').innerHTML = legendHtml;
            document.getElementById('stat-dcs').innerText = dcs.length;
        }

        function renderMonitor() {
            document.getElementById('stat-nodes').innerText = clusterData.nodes ? clusterData.nodes.length : 0;
            let totalKeys = 0;
            const grid = document.getElementById('node-grid');
            grid.innerHTML = '';

            clusterData.nodes.forEach(node => {
                totalKeys += node.key_count;
                const color = dcColorMap[node.datacenter] || '#64748b';

                let keysHtml = '';
                if(node.error) {
                     keysHtml = `<div class="text-center py-8 text-red-400 text-sm font-mono">${node.error}</div>`;
                } else if(node.keys && node.keys.length > 0) {
                    keysHtml = `
                        <div class="overflow-y-auto max-h-40 text-xs font-mono">
                            <table class="w-full text-left">
                                <thead class="text-slate-500 border-b border-slate-700"><tr><th>Key</th><th>Val</th></tr></thead>
                                <tbody>
                                    ${node.keys.map(k => `
                                        <tr class="border-b border-slate-800 hover:bg-slate-700/50">
                                            <td class="py-1 text-sky-300 truncate max-w-[80px]" title="${k.key}">${k.key}</td>
                                            <td class="py-1 text-slate-300 truncate max-w-[80px]" title="${k.value}">${k.value}</td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        </div>
                    `;
                } else {
                    keysHtml = '<div class="text-center py-8 text-slate-600 italic text-sm">Storage Empty</div>';
                }

                let statusBadge = node.status === 'ONLINE' 
                    ? '<span class="text-emerald-400 text-xs">● Online</span>'
                    : '<span class="text-red-500 text-xs">● Offline</span>';

                const card = `
                    <div class="bg-slate-800 border border-slate-700 rounded-lg overflow-hidden flex flex-col shadow-lg transition hover:border-slate-500">
                        <div class="p-3 bg-slate-900/50 border-b border-slate-700 flex justify-between items-center">
                            <div>
                                <div class="font-bold font-mono text-sm text-white">${node.address}</div>
                                <div class="text-xs text-slate-400">${node.id}</div>
                            </div>
                            <span class="text-white text-[10px] font-bold px-2 py-1 rounded-full" style="background-color:${color}">${node.datacenter}</span>
                        </div>
                        <div class="p-3 bg-slate-800/30 flex justify-between items-center border-b border-slate-700">
                            <span class="text-slate-400 text-xs">Keys: <span class="text-white font-bold">${node.key_count}</span></span>
                            ${statusBadge}
                        </div>
                        <div class="flex-1 bg-slate-900 p-2 min-h-[100px]">
                            ${keysHtml}
                        </div>
                    </div>
                `;
                grid.innerHTML += card;
            });
            document.getElementById('stat-keys').innerText = totalKeys;
        }

        function drawRing() {
            const canvas = document.getElementById('ringCanvas');
            const ctx = canvas.getContext('2d');
            const cx = canvas.width / 2;
            const cy = canvas.height / 2;
            const radius = 300;

            ctx.clearRect(0, 0, canvas.width, canvas.height);

            // Draw Base Ring
            ctx.beginPath();
            ctx.arc(cx, cy, radius, 0, 2 * Math.PI);
            ctx.strokeStyle = '#334155';
            ctx.lineWidth = 2;
            ctx.stroke();

            if(!clusterData.ring_state) return;

            // Draw Hash Ring using actual consistent hash values
            // Hash space is 0 to 2^128 (approx 3.4e38)
            const maxHash = 340282366920938463463374607431768211455n; 

            clusterData.ring_state.forEach(vnode => {
                const hashVal = BigInt(vnode.hash);
                
                // Convert huge BigInt hash to a 0-1 ratio for the circle
                // We use Number() on the ratio of BigInts. 
                // To avoid precision loss, we divide by a smaller chunk or use a scaling factor.
                // Simplified: (hash * 10000 / max) / 10000
                const scale = 1000000n;
                const scaledPos = (hashVal * scale) / maxHash;
                const ratio = Number(scaledPos) / Number(scale);
                
                const angle = ratio * 2 * Math.PI - (Math.PI / 2); // Start top

                const x = cx + Math.cos(angle) * radius;
                const y = cy + Math.sin(angle) * radius;

                const node = clusterData.nodes.find(n => n.address === vnode.node);
                const color = node ? (dcColorMap[node.datacenter] || '#94a3b8') : '#334155';

                ctx.beginPath();
                ctx.arc(x, y, 4, 0, 2 * Math.PI);
                ctx.fillStyle = color;
                ctx.fill();
            });
        }

        refreshData();
        setInterval(refreshData, 3000);
    </script>
</body>
</html>
"""

# ============================================================================
# 2. BACKEND API
# ============================================================================

def get_grpc_stub(address):
    try:
        channel = grpc.insecure_channel(address)
        return dht_pb2_grpc.DHTServiceStub(channel)
    except:
        return None

@app.get("/")
async def home():
    content = HTML_CONTENT.replace("{{ seed_node }}", SEED_NODE)
    content = content.replace("{{ replication_n }}", str(REPLICATION_N))
    return HTMLResponse(content)

@app.get("/api/cluster-state")
async def get_cluster_state():
    result = {
        "nodes": [],
        "ring_state": []
    }
    
    try:
        # 1. Get Physical Nodes
        stub = get_grpc_stub(SEED_NODE)
        ring_resp = stub.GetRing(dht_pb2.GetRingRequest(requesting_node_id="dashboard"), timeout=3)
        
        nodes_list = ring_resp.nodes
        dc_map = ring_resp.datacenter_map
        
        # 2. Use ACTUAL ConsistentHash Implementation
        # We instantiate the class to get the exact same virtual node positions
        # as the backend servers.
        ch = ConsistentHash(num_virtual_nodes=NodeConfig.NUM_VIRTUAL_NODES)
        
        for node_addr in nodes_list:
            ch.add_node(node_addr)
            
        # Extract ring state (Hash -> Node)
        # ch.ring is {hash (int): node_id (str)}
        virtual_nodes = []
        for hash_int, node_id in ch.ring.items():
            virtual_nodes.append({
                "hash": str(hash_int), # Send as string to preserve 128-bit precision in JSON
                "node": node_id
            })
            
        result["ring_state"] = virtual_nodes

        # 3. Inspect Nodes (Status & Data)
        for node_addr in nodes_list:
            node_info = {
                "address": node_addr,
                "id": f"Node_{node_addr.split(':')[1]}",
                "datacenter": dc_map.get(node_addr, "Unknown"),
                "status": "OFFLINE",
                "key_count": 0,
                "keys": [],
                "error": None
            }
            
            try:
                n_stub = get_grpc_stub(node_addr)
                inspect = n_stub.InspectNode(dht_pb2.InspectNodeRequest(), timeout=1)
                
                if inspect.success:
                    node_info["status"] = "ONLINE"
                    node_info["key_count"] = inspect.key_count
                    for entry in inspect.entries[:50]:
                        node_info["keys"].append({
                            "key": entry.key,
                            "value": entry.value,
                            "vector": entry.version_summary
                        })
            except grpc.RpcError:
                node_info["error"] = "Connection Error"
            except Exception as e:
                node_info["error"] = str(e)
            
            result["nodes"].append(node_info)

    except Exception as e:
        print(f"[Dashboard] Error: {e}")
        return {"nodes": [], "ring_state": [], "error": str(e)}

    return result

# ============================================================================
# 3. RUNNER
# ============================================================================

def start_dashboard():
    print(f"DHT DASHBOARD: http://localhost:{DASHBOARD_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=DASHBOARD_PORT, log_level="error")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DHT Dashboard')
    parser.add_argument('--port', type=int, default=9000, help='Dashboard Port')
    parser.add_argument('--seed', type=str, default='localhost:50051', help='Seed Node Address')
    parser.add_argument('--n', type=int, default=3, help='Replication Factor (N)')
    
    args = parser.parse_args()
    DASHBOARD_PORT = args.port
    SEED_NODE = args.seed
    REPLICATION_N = args.n
    
    threading.Thread(target=lambda: (time.sleep(1), webbrowser.open(f"http://localhost:{DASHBOARD_PORT}"))).start()
    start_dashboard()