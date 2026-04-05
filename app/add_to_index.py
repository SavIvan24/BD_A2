"""Add a single document to the existing Cassandra index.

Updates vocabulary, inverted_index, doc_stats, and corpus_stats tables
to incorporate the new document without rerunning the full MapReduce pipeline.
"""
import sys
import re
import os
from collections import Counter
from cassandra.cluster import Cluster


def main():
    if len(sys.argv) < 2:
        print("Usage: add_to_index.py <path_to_file>")
        sys.exit(1)

    filepath = sys.argv[1]
    filename = os.path.basename(filepath).replace('.txt', '')
    parts = filename.split('_', 1)
    doc_id = parts[0]
    doc_title = parts[1].replace('_', ' ') if len(parts) > 1 else filename

    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read()

    tokens = re.findall(r'[a-z0-9]+', text.lower())
    dl = len(tokens)
    term_counts = Counter(tokens)

    cluster_conn = Cluster(['cassandra-server'])
    session = cluster_conn.connect('search_engine')

    # Insert document stats
    session.execute(
        "INSERT INTO doc_stats (doc_id, title, dl) VALUES (%s, %s, %s)",
        [doc_id, doc_title, dl]
    )

    # Update inverted index and vocabulary
    for term, tf in term_counts.items():
        session.execute(
            "INSERT INTO inverted_index (term, doc_id, tf) VALUES (%s, %s, %s)",
            [term, doc_id, tf]
        )
        existing = session.execute(
            "SELECT df FROM vocabulary WHERE term = %s", [term]).one()
        new_df = (existing.df + 1) if existing else 1
        session.execute(
            "INSERT INTO vocabulary (term, df) VALUES (%s, %s)",
            [term, new_df]
        )

    # Update corpus stats
    row = session.execute(
        "SELECT num_docs, avg_dl FROM corpus_stats WHERE id = 1").one()
    if row:
        new_n = row.num_docs + 1
        new_avg_dl = ((row.avg_dl * row.num_docs) + dl) / new_n
    else:
        new_n = 1
        new_avg_dl = float(dl)

    session.execute(
        "INSERT INTO corpus_stats (id, num_docs, avg_dl) VALUES (1, %s, %s)",
        [new_n, new_avg_dl]
    )

    print("Indexed: [ID: {}] {} ({} tokens, {} unique terms)".format(
        doc_id, doc_title, dl, len(term_counts)))

    cluster_conn.shutdown()


if __name__ == "__main__":
    main()
