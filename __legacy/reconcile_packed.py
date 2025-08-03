#!/usr/bin/env python3
"""
reconcile_packed.py   –  PRIORITY-3 (packed_products placeholder)

This is a stub for “packed orders” reconciliation.
Fill in as requirements evolve (e.g. `packed_products` field in YAML).

Steps might include:
  • load YAML + default “packed” config
  • read a “packed” CSV
  • build a PACKED_KEY (unique_keys → extra_checks)
  • check for duplicates, summarise
  • write out `packed_with_keys.csv` + any “resolved” subsets

Right now it does nothing.
"""

import argparse
import sys
import yaml
import pandas as pd
from pathlib import Path

def main(canon_yaml, packed_csv, customers=None):
    print("⚠️  reconcile_packed.py is not yet implemented.")
    # you can start by modelling it on reconcile_orders.py / reconcile_shipments.py

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--canon-yaml", required=True)
    p.add_argument("--packed-csv", required=True)
    p.add_argument("--customers", nargs="*", help="limit to these canonical names")
    args = p.parse_args()
    main(args.canon_yaml, args.packed_csv, args.customers)
