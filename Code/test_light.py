#!/usr/bin/env python3
"""
Light Comprehensive Test - For Quick Testing
Reduced scale: 20 nodes, 2 DCs, 100 items, 3 clients
Much faster execution while still testing all features
"""

import sys
import os

# Modify the comprehensive test parameters before importing
original_file = "test_comprehensive.py"

# Read the original file
with open(original_file, 'r') as f:
    content = f.read()

# Replace the configuration values
content = content.replace("NUM_DATACENTERS = 10", "NUM_DATACENTERS = 2")
content = content.replace("NODES_PER_DC = 20", "NODES_PER_DC = 10")
content = content.replace("NUM_CLIENTS = 50", "NUM_CLIENTS = 5")
content = content.replace("NUM_ITEMS = 5000", "NUM_ITEMS = 100")

# Execute the modified content
exec(content)
