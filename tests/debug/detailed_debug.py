#!/usr/bin/env python3
"""
Detailed debug to understand the exact matching discrepancy
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

def detailed_debug():
    cfg = get_cfg("GREYSON")
    customer = "GREYSON"
    po = "4755"
    
    print("=== DETAILED RECONCILE DEBUG ===")
    
    # Extract
    order_customer_names = [cfg.get("master_order_list")] if cfg.get("master_order_list") else cfg["aliases"]
    orders = extractor.orders(order_customer_names, po)
    ships = extractor.shipments(cfg["aliases"], po)
    
    print(f"After extraction: {len(orders)} orders, {len(ships)} shipments")
    
    # Normalize
    orders = normalise.orders(orders, customer)
    ships = normalise.shipments(ships, customer)
    
    print(f"After normalization: {len(orders)} orders, {len(ships)} shipments")
    
    # Debug matching step by step
    print(f"\nDEBUG EXACT MATCHING:")
    print(f"Input shipments: {len(ships)}")
    
    exact, ships_left = match_exact.match(orders, ships, cfg)
    
    print(f"Exact matches returned: {len(exact)}")
    print(f"Ships left returned: {len(ships_left)}")
    print(f"Math check: {len(exact)} + {len(ships_left)} = {len(exact) + len(ships_left)} (should equal {len(ships)})")
    
    # Check if any rows were duplicated in matching
    if len(exact) > 0:
        print(f"Matches columns: {exact.columns.tolist()}")
        
    if len(ships_left) > 0:
        print(f"Unmatched columns: {ships_left.columns.tolist()}")
        print(f"Sample unmatched shipments:")
        print(ships_left[['Customer', 'Customer_PO', 'Style', 'Color', 'Shipping_Method']].head())

if __name__ == "__main__":
    detailed_debug()
