#!/usr/bin/env python3
"""
Debug fuzzy matching issue by testing its inputs
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

def debug_fuzzy_inputs():
    cfg = get_cfg("GREYSON")
    
    # Load and process like reconcile.py does
    order_customer_names = [cfg.get("master_order_list")] if cfg.get("master_order_list") else cfg["aliases"]
    orders = extractor.orders(order_customer_names, "4755")
    ships = extractor.shipments(cfg["aliases"], "4755")
    orders = normalise.orders(orders, "GREYSON")
    ships = normalise.shipments(ships, "GREYSON")
    
    print("=== FUZZY MATCHING INPUT DEBUG ===")
    
    # Run exact matching first
    exact, ships_left = match_exact.match(orders, ships, cfg)
    print(f"After exact matching: {len(ships_left)} ships left")
    
    # Debug fuzzy inputs
    print(f"\nFuzzy matching inputs:")
    print(f"  Orders shape: {orders.shape}")
    print(f"  Ships_left shape: {ships_left.shape}")
    print(f"  Ships_left empty: {ships_left.empty}")
    
    if not ships_left.empty:
        print(f"\nShips_left columns: {list(ships_left.columns)}")
        
        # Check for columns that fuzzy matcher expects
        expected_cols = ["CUSTOMER COLOUR DESCRIPTION", "COLOR", "Color"]
        for col in expected_cols:
            if col in ships_left.columns:
                print(f"  ✅ {col} found in ships_left")
            else:
                print(f"  ❌ {col} NOT found in ships_left")
        
        # Test what fuzzy matcher would do
        print(f"\nTesting fuzzy processing...")
        ships_processed = ships_left.copy()
        
        # Check if orders has the expected color column
        if "CUSTOMER COLOUR DESCRIPTION" in orders.columns:
            color_palette = orders["CUSTOMER COLOUR DESCRIPTION"].unique()
            print(f"  Orders color palette: {len(color_palette)} unique colors")
            print(f"  Sample colors: {list(color_palette)[:3]}")
        else:
            print(f"  ❌ Orders missing CUSTOMER COLOUR DESCRIPTION")
        
        # Check what column fuzzy matcher would pick for ships
        color_col = None
        for col in ["CUSTOMER COLOUR DESCRIPTION", "COLOR", "Color"]:
            if col in ships_processed.columns:
                color_col = col
                print(f"  ✅ Ships color column selected: {color_col}")
                break
        
        if color_col is None:
            print(f"  ❌ No color column found in ships_left!")
        
        # Test the actual error point
        try:
            # This is what fails in fuzzy matching
            ships_for_join_check = ships_left.rename(columns={v: k for k, v in cfg["map"].items()})
            join_cols = [order_col for order_col in cfg["map"].keys() 
                        if order_col in orders.columns and order_col in ships_for_join_check.columns]
            print(f"  Join columns for fuzzy: {join_cols}")
            
            # This is likely where it fails
            if len(join_cols) == 0:
                print(f"  ❌ NO JOIN COLUMNS FOUND - this would cause the error!")
            else:
                print(f"  ✅ Join columns look good")
                
        except Exception as e:
            print(f"  ❌ Error in fuzzy setup: {e}")

if __name__ == "__main__":
    debug_fuzzy_inputs()
