#!/usr/bin/env python3
"""
Analyze actual unmatched shipments to show recordlinkage potential
"""
import sys
from pathlib import Path
import pandas as pd
from ruamel.yaml import YAML

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core import extractor, normalise, match_exact

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

def analyze_unmatched_for_recordlinkage():
    cfg = get_cfg("GREYSON")
    
    # Load and process data
    order_customer_names = [cfg.get("master_order_list")] if cfg.get("master_order_list") else cfg["aliases"]
    orders = extractor.orders(order_customer_names, "4755")
    ships = extractor.shipments(cfg["aliases"], "4755")
    orders = normalise.orders(orders, "GREYSON")
    ships = normalise.shipments(ships, "GREYSON")
    
    # Run exact matching to get unmatched
    exact, ships_left = match_exact.match(orders, ships, cfg)
    
    print("=== RECORDLINKAGE POTENTIAL ANALYSIS ===")
    print(f"Total shipments: {len(ships)}")
    print(f"Exact matches: {len(exact)}")
    print(f"Unmatched shipments: {len(ships_left)}")
    
    # Analyze first few unmatched shipments for recordlinkage potential
    print(f"\n=== SAMPLE UNMATCHED SHIPMENTS ===")
    
    # Get the columns we'll be comparing
    ship_cols = ['Customer_PO', 'Style', 'Color', 'Shipping_Method']
    order_cols = ['PO NUMBER', 'CUSTOMER STYLE', 'CUSTOMER COLOUR DESCRIPTION', 'PLANNED DELIVERY METHOD']
    
    for i, (ship_idx, ship_row) in enumerate(ships_left.head(5).iterrows()):
        print(f"\n--- Unmatched Shipment #{i+1} ---")
        print(f"SHIPMENT:")
        print(f"  PO: {ship_row['Customer_PO']}")
        print(f"  Style: {ship_row['Style']}")  
        print(f"  Color: {ship_row['Color']}")
        print(f"  Delivery: {ship_row['Shipping_Method']}")
        print(f"  Qty: {ship_row['Qty']}")
        
        # Find closest orders with same PO
        matching_po_orders = orders[orders['PO NUMBER'] == ship_row['Customer_PO']]
        
        if len(matching_po_orders) > 0:
            print(f"\nCLOSEST ORDER CANDIDATES (same PO):")
            
            for j, (ord_idx, ord_row) in enumerate(matching_po_orders.head(3).iterrows()):
                print(f"  Order #{j+1}:")
                print(f"    Style: {ord_row['CUSTOMER STYLE']}")
                print(f"    Color: {ord_row['CUSTOMER COLOUR DESCRIPTION']}")
                print(f"    Delivery: {ord_row['PLANNED DELIVERY METHOD']}")
                print(f"    Qty: {ord_row.get('TOTAL QTY', ord_row.get('Qty', 'N/A'))}")
                
                # Calculate manual similarity scores
                from rapidfuzz import fuzz
                
                style_sim = fuzz.ratio(str(ship_row['Style']), str(ord_row['CUSTOMER STYLE'])) / 100
                color_sim = fuzz.ratio(str(ship_row['Color']), str(ord_row['CUSTOMER COLOUR DESCRIPTION'])) / 100
                deliv_sim = fuzz.ratio(str(ship_row['Shipping_Method']), str(ord_row['PLANNED DELIVERY METHOD'])) / 100
                
                # Quantity similarity - gaussian-like decline with distance
                ship_qty = ship_row['Qty']
                order_qty = ord_row.get('TOTAL QTY', ord_row.get('Qty', 0))
                if order_qty > 0 and ship_qty > 0:
                    # Use a 10% tolerance - slight differences don't matter much
                    tol = 0.10
                    qty_diff = abs(ship_qty - order_qty) / max(ship_qty, order_qty)
                    qty_sim = max(0, 1 - (qty_diff / tol)) if qty_diff <= tol*3 else 0
                else:
                    qty_sim = 0.5  # Medium confidence if we can't compare
                
                # Weighted score (same as our proposed recordlinkage weights)
                weighted_score = (3*style_sim + 2*color_sim + 3*1.0 + 1*deliv_sim + 1*qty_sim) / 10  # PO=1.0
                
                print(f"    SIMILARITY ANALYSIS:")
                print(f"      Style: {style_sim:.3f}")
                print(f"      Color: {color_sim:.3f}") 
                print(f"      PO: 1.000 (exact)")
                print(f"      Delivery: {deliv_sim:.3f}")
                print(f"      Qty: {qty_sim:.3f} (ship: {ship_qty}, order: {order_qty})")
                print(f"      WEIGHTED SCORE: {weighted_score:.3f}")
                
                if weighted_score >= 0.85:
                    print(f"      → HI_CONF: Would auto-match + suggest aliases")
                elif weighted_score >= 0.60:
                    print(f"      → LOW_CONF: Would queue for human review")
                else:
                    print(f"      → NO_MATCH: Would remain unmatched")
                break  # Only show first candidate
        else:
            print(f"  No orders found with PO: {ship_row['Customer_PO']}")

if __name__ == "__main__":
    analyze_unmatched_for_recordlinkage()
