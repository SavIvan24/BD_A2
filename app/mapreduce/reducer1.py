#!/usr/bin/env python3
"""Reducer for Pipeline 1: Inverted Index.

Input:  term\tdoc_id  (sorted by term)
Output: term\tdf\tdoc_id1:tf1,doc_id2:tf2,...
"""
import sys

current_term = None
doc_counts = {}


def emit(term, doc_counts):
    df = len(doc_counts)
    postings = ','.join("{}:{}".format(did, tf)
                        for did, tf in sorted(doc_counts.items()))
    print("{}\t{}\t{}".format(term, df, postings))


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 1)
    if len(parts) != 2:
        continue
    term, doc_id = parts

    if term != current_term:
        if current_term is not None:
            emit(current_term, doc_counts)
        current_term = term
        doc_counts = {}

    doc_counts[doc_id] = doc_counts.get(doc_id, 0) + 1

if current_term is not None:
    emit(current_term, doc_counts)
