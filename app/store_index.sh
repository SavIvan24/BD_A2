#!/bin/bash

echo "Store the index and others to Cassandra/ScyllaDB tables"

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

spark-submit store_index.py

echo "Done storing index in Cassandra!"
