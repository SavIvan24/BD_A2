#!/bin/bash

# Start ssh server
service ssh restart

# Starting Hadoop/YARN/Spark services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Modern pip + wheel (required for binary wheels; avoids "invalid command 'bdist_wheel'")
python -m pip install --upgrade pip setuptools wheel

# Pure-Python cassandra-driver on images without python3-dev / Python.h
export CASS_DRIVER_NO_CYTHON=1

# Install Python dependencies
pip install -r requirements.txt

# Package the virtual env for YARN distribution
venv-pack -o .venv.tar.gz

# Collect and prepare data
bash prepare_data.sh

# Run the indexer (MapReduce + Cassandra)
bash index.sh

# Run sample queries
bash search.sh "this is a query!"
