#!/bin/bash

# Usage: bash add_to_index.sh <path_to_local_file>
# The file should be named as <doc_id>_<doc_title>.txt

if [ -z "$1" ]; then
    echo "Usage: add_to_index.sh <path_to_local_file>"
    echo "File naming format: <doc_id>_<doc_title>.txt"
    exit 1
fi

if [ ! -f "$1" ]; then
    echo "ERROR: File '$1' not found"
    exit 1
fi

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

FILENAME=$(basename "$1")

# Copy file to HDFS /data
echo "Adding $FILENAME to HDFS /data..."
hdfs dfs -put -f "$1" /data/

# Update the index in Cassandra
echo "Updating index for $FILENAME..."
python3 add_to_index.py "$1"

echo "Done! Document $FILENAME added to index."
