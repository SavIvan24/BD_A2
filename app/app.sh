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

# Package the virtual env for YARN distribution (overwrite if re-running entrypoint)
rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz

# Cassandra may still be joining the ring after the container starts — wait for CQL port
echo "Waiting for Cassandra (port 9042 on cassandra-server)..."
for i in $(seq 1 72); do
    if python3 -c "import socket; s=socket.socket(); s.settimeout(2); s.connect(('cassandra-server', 9042)); s.close()" 2>/dev/null; then
        echo "Cassandra is accepting connections."
        break
    fi
    if [ "$i" -eq 72 ]; then
        echo "ERROR: Cassandra did not become ready in time."
        exit 1
    fi
    echo "  ... attempt $i/72 (sleep 5s)"
    sleep 5
done

# Collect and prepare data
bash prepare_data.sh

# Run the indexer (MapReduce + Cassandra)
bash index.sh

# Sample queries (assignment demo)
bash search.sh "This is a query!"
bash search.sh "Automation is fun"
bash search.sh "Cost of soup"
