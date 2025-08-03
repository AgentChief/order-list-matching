#!/usr/bin/env python3
"""
Analyze unmatched shipments to understand why they didn't match
"""
import pandas as pd
from pathlib import Path
from ruamel.yaml import YAML
from src.core import extractor, normalise, match_exact

yaml = YAML(typ="safe")
project_root = Path(__file__).parent
CUSTOMERS = yaml.load((project_root / "config" / "canonical_customers.yaml").read_text())["customers"]

def get_cfg(name): return next(c for c in CUSTOMERS if c["canonical"]==name)

def analyze_unmatched():
    cfg = get_cfg("GREYSON")
    
    # Load data
    orders = extractor.orders(cfg["aliases"], "4755")
    ships = extractor.shipments(cfg["aliases"], "4755")
    
    # Normalize
    orders = normalise.orders(orders, "GREYSON")
    ships = normalise.shipments(ships, "GREYSON")
    
    print(f"📊 TOTAL DATA:")
    print(f"   Orders: {len(orders)}")
    print(f"   Shipments: {len(ships)}")
    
    # Get join columns
    ships_mapped = ships.rename(columns={v: k for k, v in cfg["map"].items()})
    join_cols = [c for c in cfg["order_key_config"]["unique_keys"] if c in ships_mapped.columns]
    
    print(f"\n🔗 JOIN COLUMNS: {join_cols}")
    
    # Show unique values in each join column
    print(f"\n📋 UNIQUE VALUES IN JOIN COLUMNS:")
    for col in join_cols:
        if col in orders.columns and col in ships_mapped.columns:
            order_vals = set(orders[col].unique())
            ship_vals = set(ships_mapped[col].unique())
            
            print(f"\n   {col}:")
            print(f"     Orders ({len(order_vals)}): {sorted(order_vals)}")
            print(f"     Ships ({len(ship_vals)}): {sorted(ship_vals)}")
            
            # Show values that exist in ships but not in orders
            ship_only = ship_vals - order_vals
            if ship_only:
                print(f"     ❌ Ships ONLY: {sorted(ship_only)}")
            
            # Show values that exist in orders but not in ships
            order_only = order_vals - ship_vals
            if order_only:
                print(f"     ❌ Orders ONLY: {sorted(order_only)}")
    
    # Run exact matching to see what's left unmatched
    exact, ships_left = match_exact.match(orders, ships, cfg)
    
    print(f"\n📦 UNMATCHED SHIPMENT ANALYSIS:")
    print(f"   Total unmatched: {len(ships_left)}")
    
    if len(ships_left) > 0:
        print(f"   Unmatched shipment details:")
        for col in join_cols:
            if col in ships_left.columns:
                unmatched_vals = ships_left[col].value_counts()
                print(f"     {col}: {dict(unmatched_vals)}")
        
        print(f"\n   Sample unmatched shipments:")
        display_cols = join_cols + ['Customer', 'CartonID', 'Qty']
        available_cols = [c for c in display_cols if c in ships_left.columns]
        print(ships_left[available_cols].head(10).to_string(index=False))

if __name__ == "__main__":
    analyze_unmatched()
