"""BM25 search engine - retrieves top 10 relevant documents for a query.

Reads the inverted index and document statistics from Cassandra/ScyllaDB,
computes BM25 scores using PySpark RDD operations, and returns the ranked
results.
"""
import sys
import re
import math
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster


def main():
    query_text = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    if not query_text.strip():
        print("Usage: query.py <query_text>")
        sys.exit(1)

    query_terms = list(set(re.findall(r'[a-z0-9]+', query_text.lower())))
    if not query_terms:
        print("No valid query terms found.")
        sys.exit(0)

    print("Query: '{}'".format(query_text))
    print("Terms: {}".format(query_terms))

    cluster_conn = Cluster(['cassandra-server'])
    session = cluster_conn.connect('search_engine')

    row = session.execute(
        "SELECT num_docs, avg_dl FROM corpus_stats WHERE id = 1").one()
    if row is None:
        print("No corpus stats found. Run the indexer first.")
        cluster_conn.shutdown()
        sys.exit(1)
    N = row.num_docs
    avg_dl = row.avg_dl

    # Gather postings for all query terms: (term, doc_id, tf, df)
    postings_data = []
    for term in query_terms:
        vocab_row = session.execute(
            "SELECT df FROM vocabulary WHERE term = %s", [term]).one()
        if vocab_row is None:
            print("Term '{}' not in vocabulary, skipping.".format(term))
            continue
        df = vocab_row.df
        rows = session.execute(
            "SELECT doc_id, tf FROM inverted_index WHERE term = %s", [term])
        for r in rows:
            postings_data.append((term, r.doc_id, r.tf, df))

    if not postings_data:
        print("No matching documents found.")
        cluster_conn.shutdown()
        sys.exit(0)

    # Gather doc stats for all relevant documents
    doc_ids_needed = list(set(p[1] for p in postings_data))
    doc_stats_map = {}
    for doc_id in doc_ids_needed:
        r = session.execute(
            "SELECT title, dl FROM doc_stats WHERE doc_id = %s",
            [doc_id]).one()
        if r:
            doc_stats_map[doc_id] = (r.title, r.dl)

    cluster_conn.shutdown()

    # BM25 computation with PySpark RDD
    conf = SparkConf().setAppName("BM25 Search")
    sc = SparkContext(conf=conf)

    N_bc = sc.broadcast(N)
    avg_dl_bc = sc.broadcast(avg_dl)
    doc_stats_bc = sc.broadcast(doc_stats_map)

    postings_rdd = sc.parallelize(postings_data)

    def compute_bm25(record):
        k1 = 1.5
        b = 0.75
        term, doc_id, tf, df = record
        n_val = N_bc.value
        avgdl = avg_dl_bc.value
        ds = doc_stats_bc.value
        if doc_id not in ds:
            return (doc_id, 0.0)
        _, dl = ds[doc_id]
        idf = math.log((n_val - df + 0.5) / (df + 0.5) + 1)
        tf_norm = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl))
        return (doc_id, idf * tf_norm)

    scores_rdd = postings_rdd.map(compute_bm25)
    total_scores = scores_rdd.reduceByKey(lambda a, b: a + b)
    top_10 = total_scores.takeOrdered(10, key=lambda x: -x[1])

    print("")
    print("=" * 60)
    print("Top 10 results for: '{}'".format(query_text))
    print("=" * 60)
    for rank, (doc_id, score) in enumerate(top_10, 1):
        title = doc_stats_map.get(doc_id, ("Unknown", 0))[0]
        print("{}. [ID: {}] {} (score: {:.4f})".format(
            rank, doc_id, title, score))
    print("=" * 60)

    sc.stop()


if __name__ == "__main__":
    main()
