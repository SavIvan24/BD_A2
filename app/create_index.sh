#!/bin/bash
set -euo pipefail

echo "Create index using MapReduce pipelines"

INPUT_PATH=${1:-/input/data}
# Use the real streaming JAR under tools/lib — do not pick *-sources* / *-test* jars
# (picking test-sources.jar makes RunJar treat "-input" as the main class → ClassNotFoundException).
HADOOP_STREAMING_JAR=""
if [ -n "${HADOOP_HOME:-}" ] && [ -d "$HADOOP_HOME/share/hadoop/tools/lib" ]; then
    HADOOP_STREAMING_JAR=$(ls "$HADOOP_HOME/share/hadoop/tools/lib"/hadoop-streaming-*.jar 2>/dev/null | grep -v -- '-sources' | grep -v -- '-test' | head -1 || true)
fi
if [ -z "$HADOOP_STREAMING_JAR" ]; then
    HADOOP_STREAMING_JAR=$(find "$HADOOP_HOME" -path "*/tools/lib/hadoop-streaming*.jar" ! -name "*sources*" ! -name "*test*" 2>/dev/null | head -1 || true)
fi

if [ -z "$HADOOP_STREAMING_JAR" ] || [ ! -f "$HADOOP_STREAMING_JAR" ]; then
    echo "ERROR: Hadoop streaming JAR not found under \$HADOOP_HOME (expected share/hadoop/tools/lib/hadoop-streaming-*.jar)"
    exit 1
fi

echo "Using Hadoop Streaming JAR: $HADOOP_STREAMING_JAR"
echo "Input path: $INPUT_PATH"

hdfs dfs -mkdir -p /indexer

# Pipeline 1: Build inverted index (term -> df, postings with tf)
echo "============================================"
echo "Pipeline 1: Building Inverted Index"
echo "============================================"
hdfs dfs -rm -r -f /indexer/index

hadoop jar $HADOOP_STREAMING_JAR \
    -input $INPUT_PATH \
    -output /indexer/index \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file /app/mapreduce/mapper1.py \
    -file /app/mapreduce/reducer1.py \
    -numReduceTasks 1

echo "Pipeline 1 complete. Output:"
hdfs dfs -ls /indexer/index

# Pipeline 2: Compute document statistics (doc_id, title, length)
echo "============================================"
echo "Pipeline 2: Computing Document Statistics"
echo "============================================"
hdfs dfs -rm -r -f /indexer/doc_stats

hadoop jar $HADOOP_STREAMING_JAR \
    -input $INPUT_PATH \
    -output /indexer/doc_stats \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file /app/mapreduce/mapper2.py \
    -file /app/mapreduce/reducer2.py \
    -numReduceTasks 1

echo "Pipeline 2 complete. Output:"
hdfs dfs -ls /indexer/doc_stats

echo "============================================"
echo "All MapReduce pipelines completed!"
echo "============================================"
hdfs dfs -ls /indexer/
