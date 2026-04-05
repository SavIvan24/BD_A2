#!/usr/bin/env python3
"""Reducer for Pipeline 2: Document Statistics.

Input:  doc_id\tdoc_title\tdl  (sorted by doc_id)
Output: doc_id\tdoc_title\tdl
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if line:
        print(line)
