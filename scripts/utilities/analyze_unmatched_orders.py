#!/usr/bin/env python3
"""
Analyze Unmatched Orders Script
Identifies orders that weren't matched to shipments for quantity variance analysis
"""

import pandas as pd
import pyodbc
import sys
from pathlib import Path
sys.path.append(str(Path('.').absolute()))

from auth_helper import get_connection_string

def analyze_unmatched_orders(customer='GREYSON', po='4755'):
    """Analyze orders that weren't matched to shipments"""
    
    print(f"🔍 Analyzing Unmatched Orders for {customer} PO {po}")
    print("=" * 60)
    
    with pyodbc.connect(get_connection_string()) as conn:
        # Get all active orders with match status
        orders_query = '''
        SELECT 
            o.order_id,
            o.style_code,
            o.color_description,
            o.delivery_method,
            o.quantity,
            o.order_type,
            o.order_date,
            CASE 
                WHEN emr.order_id IS NOT NULL THEN 'MATCHED'
                ELSE 'UNMATCHED'
            END as match_status
        FROM stg_order_list o
        LEFT JOIN enhanced_matching_results emr ON o.order_id = emr.order_id
        WHERE o.customer_name LIKE ? AND o.po_number = ?
        AND o.order_type != 'CANCELLED'
        ORDER BY 
            CASE WHEN emr.order_id IS NULL THEN 0 ELSE 1 END,
            o.style_code, o.color_description
        '''
        
        df = pd.read_sql(orders_query, conn, params=[f'{customer}%', po])
        
        # Split matched vs unmatched
        matched = df[df['match_status'] == 'MATCHED']
        unmatched = df[df['match_status'] == 'UNMATCHED']
        
        print(f"📊 SUMMARY:")
        print(f"    Total Active Orders: {len(df):,}")
        print(f"    Matched Orders: {len(matched):,}")
        print(f"    Unmatched Orders: {len(unmatched):,}")
        print()
        
        # Quantity analysis
        matched_qty = matched['quantity'].sum() if len(matched) > 0 else 0
        unmatched_qty = unmatched['quantity'].sum() if len(unmatched) > 0 else 0
        total_qty = matched_qty + unmatched_qty
        
        print(f"📦 QUANTITY ANALYSIS:")
        print(f"    Total Order Quantity: {total_qty:,} units")
        print(f"    Matched Order Quantity: {matched_qty:,} units ({matched_qty/total_qty*100:.1f}%)")
        print(f"    Unmatched Order Quantity: {unmatched_qty:,} units ({unmatched_qty/total_qty*100:.1f}%)")
        print(f"    Quantity Gap: {unmatched_qty:,} units not shipped")
        print()
        
        if len(unmatched) > 0:
            print(f"📝 UNMATCHED ORDERS DETAILS ({len(unmatched)} orders):")
            print("-" * 80)
            
            # Group by style+color for better analysis
            unmatched_summary = unmatched.groupby(['style_code', 'color_description', 'delivery_method']).agg({
                'quantity': 'sum',
                'order_id': 'count'
            }).rename(columns={'order_id': 'order_count'}).reset_index()
            
            for _, row in unmatched_summary.iterrows():
                print(f"Style: {row['style_code']}")
                print(f"Color: {row['color_description']}")
                print(f"Delivery: {row['delivery_method']}")
                print(f"Total Qty: {row['quantity']:,} units ({row['order_count']} orders)")
                print()
        
        # Get shipment data for comparison
        shipments_query = '''
        SELECT 
            shipment_id,
            style_code,
            color_description,
            delivery_method,
            quantity,
            shipped_date
        FROM stg_fm_orders_shipped_table
        WHERE customer_name LIKE ? AND po_number = ?
        ORDER BY style_code, color_description
        '''
        
        shipments_df = pd.read_sql(shipments_query, conn, params=[f'{customer}%', po])
        
        print(f"🚚 SHIPMENT COMPARISON:")
        print(f"    Total Shipments: {len(shipments_df):,}")
        print(f"    Total Shipped Quantity: {shipments_df['quantity'].sum():,} units")
        print()
        
        # Identify potential manual matches
        if len(unmatched) > 0:
            print(f"🔎 POTENTIAL MANUAL MATCH OPPORTUNITIES:")
            print("-" * 50)
            
            # Look for shipments that might match unmatched orders
            for _, order in unmatched.iterrows():
                # Look for shipments with same style
                style_matches = shipments_df[shipments_df['style_code'] == order['style_code']]
                if len(style_matches) > 0:
                    print(f"Order Style {order['style_code']} ({order['color_description']}, {order['quantity']} units)")
                    print(f"  Potential shipment matches:")
                    for _, ship in style_matches.iterrows():
                        print(f"    Shipment {ship['shipment_id']}: {ship['color_description']}, {ship['quantity']} units")
                    print()
        
        return {
            'total_orders': len(df),
            'matched_orders': len(matched),
            'unmatched_orders': len(unmatched),
            'matched_qty': matched_qty,
            'unmatched_qty': unmatched_qty,
            'unmatched_details': unmatched,
            'shipments_total': len(shipments_df),
            'shipments_qty': shipments_df['quantity'].sum()
        }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze unmatched orders for quantity variance investigation')
    parser.add_argument('--customer', default='GREYSON', help='Customer name (default: GREYSON)')
    parser.add_argument('--po', default='4755', help='PO number (default: 4755)')
    
    args = parser.parse_args()
    
    try:
        results = analyze_unmatched_orders(args.customer, args.po)
        print(f"✅ Analysis complete!")
        
        if results['unmatched_orders'] > 0:
            print(f"\n🎯 KEY FINDINGS:")
            print(f"    {results['unmatched_qty']:,} units in orders have no corresponding shipments")
            print(f"    This represents {results['unmatched_orders']} unmatched orders")
            print(f"    Review the potential manual matches above for investigation")
        else:
            print(f"\n✅ All orders are matched to shipments!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
