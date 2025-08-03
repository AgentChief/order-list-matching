#!/usr/bin/env python3
"""
Analysis of GREYSON matching logic - shows exactly what columns are used for matching
and how success is defined.
"""
import argparse, datetime as dt
from pathlib import Path
from ruamel.yaml import YAML
import pandas as pd
from tqdm import tqdm

from src.core import extractor, normalise, match_exact, match_fuzzy, match_llm

yaml = YAML(typ="safe")
# Get the path to the project root (order-match-lm/) from this file's location
project_root = Path(__file__).parent
CUSTOMERS = yaml.load((project_root / "config" / "canonical_customers.yaml").read_text())["customers"]
CFG       = yaml.load((project_root / "config" / "config.yaml").read_text())

def get_cfg(name): return next(c for c in CUSTOMERS if c["canonical"]==name)

def analyze_matching_logic(customer, po):
    cfg = get_cfg(customer)
    
    print(f"🔍 MATCHING ANALYSIS FOR {customer} - PO {po}")
    print("=" * 80)
    
    print(f"\n📋 CONFIGURATION:")
    print(f"   Customer: {customer}")
    print(f"   Aliases: {cfg['aliases']}")
    print(f"   Status: {cfg['status']}")
    
    print(f"\n🗝️  ORDER KEY CONFIG:")
    print(f"   Unique Keys (for exact matching): {cfg['order_key_config']['unique_keys']}")
    print(f"   Extra Checks: {cfg['order_key_config']['extra_checks']}")
    
    if 'shipment_key_config' in cfg:
        print(f"\n📦 SHIPMENT KEY CONFIG:")
        print(f"   Unique Keys: {cfg['shipment_key_config']['unique_keys']}")
        print(f"   Extra Checks: {cfg['shipment_key_config']['extra_checks']}")
    
    print(f"\n🔗 COLUMN MAPPING (Ships → Orders):")
    for ship_col, order_col in cfg['map'].items():
        print(f"   {ship_col} → {order_col}")
    
    # Load and process data
    print(f"\n📊 DATA LOADING:")
    orders = extractor.orders(cfg["aliases"], po)
    ships  = extractor.shipments(cfg["aliases"], po)
    print(f"   Orders loaded: {len(orders)} rows")
    print(f"   Shipments loaded: {len(ships)} rows")
    
    # Normalize
    orders = normalise.orders(orders, customer)
    ships  = normalise.shipments(ships, customer)
    print(f"   After normalization: {len(orders)} orders, {len(ships)} shipments")
    
    # Show sample data before matching
    print(f"\n📋 SAMPLE ORDER DATA (key columns):")
    order_key_cols = cfg['order_key_config']['unique_keys']
    if all(col in orders.columns for col in order_key_cols):
        sample_orders = orders[order_key_cols].head(3)
        for i, row in sample_orders.iterrows():
            print(f"   Order {i+1}: {dict(row)}")
    
    print(f"\n📦 SAMPLE SHIPMENT DATA (before mapping):")
    ship_cols = ['Customer_PO', 'Shipping_Method', 'Style', 'Color', 'Size']
    if all(col in ships.columns for col in ship_cols):
        sample_ships = ships[ship_cols].head(3)
        for i, row in sample_ships.iterrows():
            print(f"   Shipment {i+1}: {dict(row)}")
    
    # Apply exact matching
    print(f"\n🎯 EXACT MATCHING PROCESS:")
    
    # Show what columns will be renamed
    print(f"   Column mapping applied:")
    for ship_col, order_col in cfg['map'].items():
        if ship_col in ships.columns:
            print(f"     '{ship_col}' → '{order_col}'")
    
    # Apply the mapping
    ships_mapped = ships.rename(columns={v: k for k, v in cfg["map"].items()})
    
    # Show which columns will be used for joining
    join_cols = [c for c in cfg["order_key_config"]["unique_keys"] if c in ships_mapped.columns]
    print(f"   Join columns (exact match): {join_cols}")
    
    # Perform exact matching
    exact, ships_left = match_exact.match(orders, ships, cfg)
    
    print(f"\n📊 MATCHING RESULTS:")
    print(f"   Exact matches: {len(exact)}")
    print(f"   Unmatched shipments: {len(ships_left)}")
    print(f"   Match rate: {len(exact)/(len(ships)) * 100:.1f}%")
    
    # Show matching success criteria
    print(f"\n✅ SUCCESS CRITERIA:")
    print(f"   EXACT MATCH: Records are considered a match if ALL of these columns are identical:")
    for i, col in enumerate(join_cols, 1):
        print(f"     {i}. {col}")
    
    print(f"\n   FUZZY MATCH: If exact match fails, fuzzy matching uses:")
    print(f"     - Color similarity (using fuzzy string matching)")
    print(f"     - Same join columns as exact match, but with color tolerance")
    print(f"     - Confidence threshold: {cfg.get('fuzzy_threshold', 0.9) * 100}%")
    
    # Show sample matches
    if len(exact) > 0:
        print(f"\n🔍 SAMPLE EXACT MATCHES:")
        for i, (idx, match) in enumerate(exact.head(3).iterrows()):
            print(f"   Match {i+1}:")
            for col in join_cols:
                order_val = match.get(col, 'N/A')
                print(f"     {col}: {order_val}")
            print(f"     Method: {match.get('method', 'N/A')}")
            print(f"     Confidence: {match.get('confidence', 'N/A')}")
            print()
    
    print(f"\n📈 SUMMARY:")
    print(f"   Total Shipments: {len(ships)}")
    print(f"   Successfully Matched: {len(exact)}")
    print(f"   Match Success Rate: {len(exact)/len(ships)*100:.1f}%")
    print(f"   Matching Method: {'Exact only' if len(ships_left) == 0 else 'Exact + Fuzzy'}")

if __name__ == "__main__":
    analyze_matching_logic("GREYSON", "4755")
