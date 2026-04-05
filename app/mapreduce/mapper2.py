#!/usr/bin/env python3
"""Mapper for Pipeline 2: Document Statistics.

Input:  doc_id\tdoc_title\tdoc_text
Output: doc_id\tdoc_title\tdl
"""
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 2)
    if len(parts) < 3:
        continue
    doc_id = parts[0]
    doc_title = parts[1]
    doc_text = parts[2]
    tokens = re.findall(r'[a-z0-9]+', doc_text.lower())
    dl = len(tokens)
    print("{}\t{}\t{}".format(doc_id, doc_title, dl))
