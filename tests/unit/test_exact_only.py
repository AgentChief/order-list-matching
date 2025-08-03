#!/usr/bin/env python3
"""
Test just exact matching without fuzzy to isolate the issue
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
    customer_cfg = next(c for c in CUSTOMERS if c["canonical"]==name)
    if "map" not in customer_cfg and "map" in GLOBAL_CONFIG:
        customer_cfg["map"] = GLOBAL_CONFIG["map"].copy()
    if "order_key_config" not in customer_cfg and "order_key_config" in GLOBAL_CONFIG:
        customer_cfg["order_key_config"] = GLOBAL_CONFIG["order_key_config"].copy()
    if "shipment_key_config" not in customer_cfg and "shipment_key_config" in GLOBAL_CONFIG:
        customer_cfg["shipment_key_config"] = GLOBAL_CONFIG["shipment_key_config"].copy()
    return customer_cfg

def test_exact_only():
    cfg = get_cfg("GREYSON")
    
    # Load data
    order_customer_names = [cfg.get("master_order_list")] if cfg.get("master_order_list") else cfg["aliases"]
    orders = extractor.orders(order_customer_names, "4755")
    ships = extractor.shipments(cfg["aliases"], "4755")
    orders = normalise.orders(orders, "GREYSON")
    ships = normalise.shipments(ships, "GREYSON")
    
    print("=== EXACT MATCHING ONLY TEST ===")
    print(f"Input: {len(orders)} orders, {len(ships)} shipments")
    
    # Test exact matching
    exact, ships_left = match_exact.match(orders, ships, cfg)
    
    print(f"Results:")
    print(f"  Exact matches: {len(exact)}")
    print(f"  Ships left: {len(ships_left)}")
    print(f"  Total check: {len(exact)} + {len(ships_left)} = {len(exact) + len(ships_left)} (should be {len(ships)})")
    
    # Check leftover structure
    print(f"\nLeftover structure check:")
    print(f"  Columns: {list(ships_left.columns)}")
    print(f"  Sample data:")
    if len(ships_left) > 0:
        print(ships_left[['Customer', 'Customer_PO', 'Style', 'Color', 'Shipping_Method']].head(3))
    
    # Calculate match rate
    match_rate = len(exact) / len(ships) * 100
    print(f"\nMatch Rate: {match_rate:.1f}%")
    
    return exact, ships_left

if __name__ == "__main__":
    test_exact_only()
