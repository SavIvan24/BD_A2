#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

# If parquet file exists, generate documents from it
if [ -f a.parquet ]; then
    echo "Found parquet file, uploading to HDFS..."
    hdfs dfs -put -f a.parquet /
    echo "Generating document files from parquet..."
    spark-submit prepare_data.py
fi

# Put document files to HDFS /data
echo "Putting documents to HDFS /data..."
hdfs dfs -rm -r -f /data
hdfs dfs -put data /
echo "Documents in HDFS /data:"
hdfs dfs -ls /data | head -20

# Consolidate documents into /input/data for MapReduce (PySpark RDD)
echo "Consolidating data into /input/data for MapReduce..."
hdfs dfs -rm -r -f /input
spark-submit consolidate_data.py

echo "Consolidated input data:"
hdfs dfs -ls /input/data
echo "Done data preparation!"
