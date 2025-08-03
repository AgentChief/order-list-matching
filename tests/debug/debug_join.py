#!/usr/bin/env python3
"""
Debug why only 2 columns are being used for matching
"""
from pathlib import Path
from ruamel.yaml import YAML
from src.core import extractor, normalise

yaml = YAML(typ="safe")
project_root = Path(__file__).parent
CUSTOMERS = yaml.load((project_root / "config" / "canonical_customers.yaml").read_text())["customers"]

def get_cfg(name): return next(c for c in CUSTOMERS if c["canonical"]==name)

def debug_join_columns():
    cfg = get_cfg("GREYSON")
    
    print("🔍 DEBUGGING JOIN COLUMN SELECTION")
    print("=" * 50)
    
    print(f"\n📋 GREYSON CONFIG:")
    print(f"   Order unique_keys: {cfg['order_key_config']['unique_keys']}")
    print(f"   Mapping: {cfg['map']}")
    
    # Load data
    orders = extractor.orders(cfg["aliases"], "4755")
    ships = extractor.shipments(cfg["aliases"], "4755")
    
    # Normalize
    orders = normalise.orders(orders, "GREYSON")
    ships = normalise.shipments(ships, "GREYSON")
    
    print(f"\n📦 ORIGINAL SHIPMENT COLUMNS:")
    print(f"   {list(ships.columns)}")
    
    # Apply the mapping
    ships_mapped = ships.rename(columns={v: k for k, v in cfg["map"].items()})
    
    print(f"\n🔗 AFTER MAPPING - SHIPMENT COLUMNS:")
    print(f"   {list(ships_mapped.columns)}")
    
    print(f"\n🎯 JOIN COLUMN ANALYSIS:")
    for col in cfg['order_key_config']['unique_keys']:
        in_orders = col in orders.columns
        in_ships = col in ships_mapped.columns
        status = "✅" if (in_orders and in_ships) else "❌"
        
        print(f"   {status} {col}:")
        print(f"      In Orders: {in_orders}")
        print(f"      In Ships (after mapping): {in_ships}")
        
        if not in_ships and col in cfg["map"]:
            ship_col = cfg["map"][col]
            print(f"      Original ship column '{ship_col}' exists: {ship_col in ships.columns}")
    
    # Show what gets selected
    join_cols = [c for c in cfg["order_key_config"]["unique_keys"] if c in ships_mapped.columns]
    print(f"\n🔗 FINAL JOIN COLUMNS SELECTED: {join_cols}")
    
    print(f"\n💡 WHY ONLY {len(join_cols)} COLUMNS?")
    for col in cfg['order_key_config']['unique_keys']:
        if col not in join_cols:
            print(f"   ❌ {col}: Not available in shipments after mapping")
        else:
            print(f"   ✅ {col}: Available for joining")

if __name__ == "__main__":
    debug_join_columns()
