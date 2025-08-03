#!/usr/bin/env python3
"""
Debug what columns are actually used for matching
"""
import pandas as pd
from pathlib import Path
from ruamel.yaml import YAML
from src.core import extractor, normalise, match_exact

yaml = YAML(typ="safe")
project_root = Path(__file__).parent
CUSTOMERS = yaml.load((project_root / "config" / "canonical_customers.yaml").read_text())["customers"]
GLOBAL_CONFIG = yaml.load((project_root / "config" / "canonical_customers.yaml").read_text()).get("global_config", {})

def get_cfg(name): 
    """Get customer configuration with global fallbacks"""
    customer_cfg = next(c for c in CUSTOMERS if c["canonical"]==name)
    
    # Apply global fallbacks if customer doesn't have specific configs
    if "map" not in customer_cfg and "map" in GLOBAL_CONFIG:
        customer_cfg["map"] = GLOBAL_CONFIG["map"].copy()
        print(f"ℹ️  Using global map for {name}")
    
    if "order_key_config" not in customer_cfg and "order_key_config" in GLOBAL_CONFIG:
        customer_cfg["order_key_config"] = GLOBAL_CONFIG["order_key_config"].copy()
        print(f"ℹ️  Using global order_key_config for {name}")
    
    if "shipment_key_config" not in customer_cfg and "shipment_key_config" in GLOBAL_CONFIG:
        customer_cfg["shipment_key_config"] = GLOBAL_CONFIG["shipment_key_config"].copy()
        print(f"ℹ️  Using global shipment_key_config for {name}")
    
    return customer_cfg

def debug_matching():
    cfg = get_cfg("GREYSON")
    
    print(f"\n🔧 GREYSON CONFIGURATION:")
    print(f"   Map: {cfg.get('map', 'NOT DEFINED')}")
    print(f"   Order Key Config: {cfg.get('order_key_config', 'NOT DEFINED')}")
    
    # Load data
    orders = extractor.orders(cfg["aliases"], "4755")
    ships = extractor.shipments(cfg["aliases"], "4755")
    
    # Normalize
    orders = normalise.orders(orders, "GREYSON")
    ships = normalise.shipments(ships, "GREYSON")
    
    print(f"\n📊 DATA LOADED:")
    print(f"   Orders: {len(orders)}")
    print(f"   Shipments: {len(ships)}")
    
    # Show what the actual matching code will use
    print(f"\n🔗 WHAT MATCH_EXACT.PY USES:")
    print(f"   cfg['map'].keys(): {list(cfg['map'].keys())}")
    
    # Rename shipment cols as match_exact.py does
    ships_mapped = ships.rename(columns={v: k for k, v in cfg["map"].items()})
    
    # Get join columns as match_exact.py does
    join_cols = [order_col for order_col in cfg["map"].keys() 
                 if order_col in orders.columns and order_col in ships_mapped.columns]
    
    print(f"   Join columns: {join_cols}")
    
    # Show what analyze_unmatched.py INCORRECTLY uses
    print(f"\n❌ WHAT ANALYZE_UNMATCHED.PY INCORRECTLY USES:")
    wrong_join_cols = [c for c in cfg["order_key_config"]["unique_keys"] if c in ships_mapped.columns]
    print(f"   Wrong join columns: {wrong_join_cols}")
    
    # Show unique values in CORRECT join columns
    print(f"\n📋 UNIQUE VALUES IN CORRECT JOIN COLUMNS:")
    for col in join_cols:
        order_vals = set(orders[col].unique())
        ship_vals = set(ships_mapped[col].unique())
        
        print(f"\n   {col}:")
        print(f"     Orders ({len(order_vals)}): {sorted(order_vals)}")
        print(f"     Ships ({len(ship_vals)}): {sorted(ship_vals)}")
        
        # Show mismatches
        ship_only = ship_vals - order_vals
        if ship_only:
            print(f"     ❌ Ships ONLY: {sorted(ship_only)}")
        
        order_only = order_vals - ship_vals
        if order_only:
            print(f"     ❌ Orders ONLY: {sorted(order_only)}")

if __name__ == "__main__":
    debug_matching()
