#!/bin/bash

echo "=========================================="
echo "Running the complete indexing pipeline"
echo "=========================================="

INPUT_PATH=${1:-/input/data}

# Step 1: Create index in HDFS using MapReduce
echo "Step 1: Creating index with MapReduce..."
bash create_index.sh $INPUT_PATH

# Step 2: Store index in Cassandra/ScyllaDB
echo "Step 2: Storing index in Cassandra..."
bash store_index.sh

echo "=========================================="
echo "Indexing complete!"
echo "=========================================="
