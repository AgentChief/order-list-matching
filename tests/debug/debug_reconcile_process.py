#!/usr/bin/env python3
"""
Debug the complete reconcile process to find where shipments get filtered
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

def debug_reconcile_process():
    cfg = get_cfg("GREYSON")
    customer = "GREYSON"
    po = "4755"
    
    print("=== STEP 1: EXTRACT DATA ===")
    
    # Extract like reconcile.py does
    order_customer_names = [cfg.get("master_order_list")] if cfg.get("master_order_list") else cfg["aliases"]
    print(f"Order customer names: {order_customer_names}")
    
    orders = extractor.orders(order_customer_names, po)
    ships = extractor.shipments(cfg["aliases"], po)
    
    print(f"After extraction:")
    print(f"   Orders: {len(orders)}")
    print(f"   Shipments: {len(ships)}")
    
    if len(ships) > 0:
        print(f"   Ship columns: {list(ships.columns)}")
    
    print("\n=== STEP 2: NORMALIZE ===")
    
    # Normalize like reconcile.py does
    orders = normalise.orders(orders, customer)
    ships = normalise.shipments(ships, customer)
    
    print(f"After normalization:")
    print(f"   Orders: {len(orders)}")
    print(f"   Shipments: {len(ships)}")
    
    print("\n=== STEP 3: MATCH ===")
    
    # Match like reconcile.py does
    exact, ships_left = match_exact.match(orders, ships, cfg)
    
    print(f"After exact matching:")
    print(f"   Exact matches: {len(exact)}")
    print(f"   Ships left: {len(ships_left)}")
    
    # Show join columns
    ships_for_join_check = ships.rename(columns={v: k for k, v in cfg["map"].items()})
    join_cols = [order_col for order_col in cfg["map"].keys() 
                 if order_col in orders.columns and order_col in ships_for_join_check.columns]
    print(f"   Join columns: {join_cols}")

if __name__ == "__main__":
    debug_reconcile_process()
