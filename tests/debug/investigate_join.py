#!/usr/bin/env python3
"""
Investigate specific join failures to understand the duplication issue
"""
import pandas as pd
from pathlib import Path
from ruamel.yaml import YAML
from src.core import extractor, normalise

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
    
    if "order_key_config" not in customer_cfg and "order_key_config" in GLOBAL_CONFIG:
        customer_cfg["order_key_config"] = GLOBAL_CONFIG["order_key_config"].copy()
    
    if "shipment_key_config" not in customer_cfg and "shipment_key_config" in GLOBAL_CONFIG:
        customer_cfg["shipment_key_config"] = GLOBAL_CONFIG["shipment_key_config"].copy()
    
    return customer_cfg

def investigate_join_issue():
    cfg = get_cfg("GREYSON")
    
    # Extract and normalize
    order_customer_names = [cfg.get("master_order_list")] if cfg.get("master_order_list") else cfg["aliases"]
    orders = extractor.orders(order_customer_names, "4755")
    ships = extractor.shipments(cfg["aliases"], "4755")
    orders = normalise.orders(orders, "GREYSON")
    ships = normalise.shipments(ships, "GREYSON")
    
    print("=== JOIN ISSUE INVESTIGATION ===")
    
    # Look for the specific failing case: LSP24K59
    print("\n1. ORDERS WITH LSP24K59:")
    lsp_orders = orders[orders['CUSTOMER STYLE'] == 'LSP24K59']
    if len(lsp_orders) > 0:
        print(f"Found {len(lsp_orders)} orders with LSP24K59")
        print("Sample order columns:")
        join_cols = ['PO NUMBER', 'PLANNED DELIVERY METHOD', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION']
        print(lsp_orders[join_cols].head())
    else:
        print("❌ NO orders found with exact style 'LSP24K59'")
        print("Checking for style variants...")
        lsp_variants = orders[orders['CUSTOMER STYLE'].str.contains('LSP24K59', na=False)]
        print(f"Found {len(lsp_variants)} order variants:")
        if len(lsp_variants) > 0:
            print(lsp_variants[['CUSTOMER STYLE'] + join_cols].head())
    
    print("\n2. SHIPMENTS WITH LSP24K59:")
    # Rename shipments for comparison
    ships_renamed = ships.rename(columns={v: k for k, v in cfg["map"].items()})
    lsp_ships = ships_renamed[ships_renamed['CUSTOMER STYLE'] == 'LSP24K59']
    if len(lsp_ships) > 0:
        print(f"Found {len(lsp_ships)} shipments with LSP24K59")
        print("Sample shipment columns:")
        print(lsp_ships[join_cols].head())
    else:
        print("❌ NO shipments found with exact style 'LSP24K59' after renaming")
        print("Original shipments with LSP24K59:")
        orig_lsp = ships[ships['Style'] == 'LSP24K59']
        print(f"Found {len(orig_lsp)} original shipments")
        if len(orig_lsp) > 0:
            print(orig_lsp[['Customer_PO', 'Style', 'Color', 'Shipping_Method']].head())
    
    print("\n3. JOIN COLUMN AVAILABILITY:")
    join_cols = ['PO NUMBER', 'PLANNED DELIVERY METHOD', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION']
    for col in join_cols:
        in_orders = col in orders.columns
        in_ships = col in ships_renamed.columns
        print(f"   {col}: Orders={in_orders}, Ships={in_ships}")
        if in_orders and in_ships:
            order_vals = set(orders[col].unique())
            ship_vals = set(ships_renamed[col].unique())
            print(f"     Order values: {len(order_vals)}, Ship values: {len(ship_vals)}")
    
    print("\n4. DUPLICATION CHECK:")
    print("Checking for duplicate orders...")
    order_dupes = orders.duplicated(subset=join_cols, keep=False)
    print(f"Duplicate orders on join columns: {order_dupes.sum()}")
    
    print("Checking for duplicate shipments...")
    ship_dupes = ships_renamed.duplicated(subset=join_cols, keep=False)
    print(f"Duplicate shipments on join columns: {ship_dupes.sum()}")
    
    if ship_dupes.sum() > 0:
        print("Sample duplicate shipments:")
        print(ships_renamed[ship_dupes][join_cols].head())

if __name__ == "__main__":
    investigate_join_issue()
