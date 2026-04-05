"""Read index data from HDFS and store in Cassandra/ScyllaDB tables.

Cassandra schema:
  - vocabulary(term, df)           : unique terms with document frequency
  - inverted_index(term, doc_id, tf): postings list per term
  - doc_stats(doc_id, title, dl)   : per-document metadata
  - corpus_stats(id, num_docs, avg_dl): global corpus statistics
"""
from pyspark import SparkContext
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time


def connect_cassandra(hosts, max_retries=30, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            cluster = Cluster(hosts)
            session = cluster.connect()
            print("Connected to Cassandra")
            return cluster, session
        except Exception as e:
            print("Waiting for Cassandra... attempt {}/{} ({})".format(
                attempt, max_retries, e))
            time.sleep(delay)
    raise RuntimeError("Could not connect to Cassandra")


def main():
    sc = SparkContext(appName="Store Index", master="local")

    print("Reading inverted index from HDFS...")
    index_data = sc.textFile("/indexer/index/part-*").collect()

    print("Reading document stats from HDFS...")
    doc_stats_data = sc.textFile("/indexer/doc_stats/part-*").collect()

    sc.stop()

    cluster, session = connect_cassandra(['cassandra-server'])

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('search_engine')

    session.execute("DROP TABLE IF EXISTS vocabulary")
    session.execute("DROP TABLE IF EXISTS inverted_index")
    session.execute("DROP TABLE IF EXISTS doc_stats")
    session.execute("DROP TABLE IF EXISTS corpus_stats")

    session.execute("""
        CREATE TABLE vocabulary (
            term text PRIMARY KEY,
            df int
        )
    """)
    session.execute("""
        CREATE TABLE inverted_index (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE doc_stats (
            doc_id text PRIMARY KEY,
            title text,
            dl int
        )
    """)
    session.execute("""
        CREATE TABLE corpus_stats (
            id int PRIMARY KEY,
            num_docs int,
            avg_dl double
        )
    """)

    # Insert inverted index and vocabulary
    vocab_stmt = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    index_stmt = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)")

    print("Inserting inverted index ({} terms)...".format(len(index_data)))
    for i, line in enumerate(index_data):
        if not line.strip():
            continue
        parts = line.strip().split('\t')
        if len(parts) != 3:
            continue
        term, df_str, postings_str = parts
        df = int(df_str)
        session.execute(vocab_stmt, [term, df])

        for posting in postings_str.split(','):
            doc_tf = posting.split(':')
            if len(doc_tf) == 2:
                doc_id, tf = doc_tf[0], int(doc_tf[1])
                session.execute(index_stmt, [term, doc_id, tf])

        if (i + 1) % 5000 == 0:
            print("  processed {}/{} terms".format(i + 1, len(index_data)))

    # Insert document stats and compute corpus-level stats
    doc_stmt = session.prepare(
        "INSERT INTO doc_stats (doc_id, title, dl) VALUES (?, ?, ?)")

    total_dl = 0
    num_docs = 0

    print("Inserting document stats ({} docs)...".format(len(doc_stats_data)))
    for line in doc_stats_data:
        if not line.strip():
            continue
        parts = line.strip().split('\t')
        if len(parts) != 3:
            continue
        doc_id, title, dl_str = parts
        dl = int(dl_str)
        session.execute(doc_stmt, [doc_id, title, dl])
        total_dl += dl
        num_docs += 1

    avg_dl = total_dl / num_docs if num_docs > 0 else 0.0
    session.execute(
        SimpleStatement(
            "INSERT INTO corpus_stats (id, num_docs, avg_dl) VALUES (1, %s, %s)"),
        [num_docs, avg_dl]
    )

    print("Corpus stats: N={}, avg_dl={:.2f}".format(num_docs, avg_dl))
    print("Index stored in Cassandra successfully!")

    cluster.shutdown()


if __name__ == "__main__":
    main()
