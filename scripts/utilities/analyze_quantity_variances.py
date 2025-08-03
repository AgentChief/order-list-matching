#!/usr/bin/env python3
"""
Analyze quantity variances and show unmatched orders for investigation
Focus on FAILED quantity matches (>10% variance) and potential resolutions
"""
import pandas as pd
import pyodbc
from auth_helper import get_connection_string

def analyze_quantity_variances():
    print("🔍 QUANTITY VARIANCE ANALYSIS - GREYSON PO 4755")
    print("=" * 60)
    
    with pyodbc.connect(get_connection_string()) as conn:
        
        # 1. Get all FAILED quantity matches
        print("\n❌ FAILED QUANTITY MATCHES (>10% variance):")
        failed_matches_query = """
        SELECT 
            shipment_id,
            order_id,
            shipment_style_code,
            order_style_code,
            shipment_color_description,
            order_color_description,
            shipment_quantity,
            order_quantity,
            quantity_check_result,
            match_layer,
            confidence_score
        FROM enhanced_matching_results
        WHERE customer_name = 'GREYSON' 
        AND po_number = '4755'
        AND quantity_check_result = 'FAIL'
        ORDER BY ABS(shipment_quantity - order_quantity) DESC
        """
        
        failed_df = pd.read_sql(failed_matches_query, conn)
        print(f"Total failed matches: {len(failed_df)}")
        
        if len(failed_df) > 0:
            failed_df['qty_variance'] = failed_df['shipment_quantity'] - failed_df['order_quantity']
            failed_df['qty_variance_pct'] = (failed_df['qty_variance'] / failed_df['order_quantity'] * 100).round(1)
            
            print("\n📊 Failed matches breakdown:")
            print(failed_df[['shipment_id', 'shipment_style_code', 'shipment_quantity', 
                           'order_quantity', 'qty_variance', 'qty_variance_pct']].to_string(index=False))
            
            # 2. For each failed match, find potential unmatched orders
            print(f"\n🔍 INVESTIGATING UNMATCHED ORDERS FOR QUANTITY RESOLUTION:")
            print("=" * 80)
            
            for idx, row in failed_df.iterrows():
                print(f"\n--- FAILED MATCH {idx+1}: Shipment {row['shipment_id']} ---")
                print(f"Style: {row['shipment_style_code']}, Color: {row['shipment_color_description']}")
                print(f"Variance: {row['qty_variance']} units ({row['qty_variance_pct']}%)")
                
                # Find unmatched ACTIVE orders with same style/color
                unmatched_active_query = """
                SELECT 
                    o.order_id,
                    o.style_code,
                    o.color_description,
                    o.quantity,
                    o.delivery_method,
                    o.order_type
                FROM stg_order_list o
                LEFT JOIN enhanced_matching_results m ON o.order_id = m.order_id
                WHERE o.customer_name = 'GREYSON'
                AND o.po_number = '4755'
                AND o.order_type = 'ACTIVE'
                AND m.order_id IS NULL
                AND (
                    o.style_code = ? OR
                    o.color_description = ?
                )
                ORDER BY o.quantity DESC
                """
                
                unmatched_df = pd.read_sql(unmatched_active_query, conn, 
                                         params=[row['shipment_style_code'], row['shipment_color_description']])
                
                if len(unmatched_df) > 0:
                    print(f"✅ Found {len(unmatched_df)} potential ACTIVE orders:")
                    print(unmatched_df[['order_id', 'style_code', 'color_description', 
                                     'quantity', 'delivery_method']].to_string(index=False))
                    
                    # Check if any combinations could resolve the variance
                    total_unmatched_qty = unmatched_df['quantity'].sum()
                    if abs(total_unmatched_qty - abs(row['qty_variance'])) <= 50:  # Within 50 units
                        print(f"🎯 POTENTIAL RESOLUTION: {total_unmatched_qty} unmatched units could resolve {abs(row['qty_variance'])} variance")
                else:
                    print("❌ No unmatched ACTIVE orders found with matching style/color")
                
                # Also check CANCELLED orders for historical context
                cancelled_query = """
                SELECT 
                    o.order_id,
                    o.style_code,
                    o.color_description,
                    o.quantity,
                    o.delivery_method,
                    o.order_type
                FROM stg_order_list o
                WHERE o.customer_name = 'GREYSON'
                AND o.po_number = '4755'
                AND o.order_type = 'CANCELLED'
                AND (
                    o.style_code = ? OR
                    o.color_description = ?
                )
                ORDER BY o.quantity DESC
                """
                
                cancelled_df = pd.read_sql(cancelled_query, conn,
                                         params=[row['shipment_style_code'], row['shipment_color_description']])
                
                if len(cancelled_df) > 0:
                    print(f"📋 Found {len(cancelled_df)} CANCELLED orders (historical context):")
                    print(cancelled_df[['order_id', 'style_code', 'color_description', 'quantity']].to_string(index=False))
                    
                    cancelled_qty = cancelled_df['quantity'].sum()
                    if abs(cancelled_qty - abs(row['qty_variance'])) <= 50:
                        print(f"💡 CANCELLED orders ({cancelled_qty} units) might explain variance")
        
        # 3. Summary of all unmatched orders for potential Layer 3/4 matching
        print(f"\n📊 SUMMARY: ALL UNMATCHED ORDERS FOR LAYER 3/4 MATCHING")
        print("=" * 60)
        
        unmatched_summary_query = """
        SELECT 
            o.order_type,
            COUNT(*) as order_count,
            SUM(o.quantity) as total_quantity
        FROM stg_order_list o
        LEFT JOIN enhanced_matching_results m ON o.order_id = m.order_id
        WHERE o.customer_name = 'GREYSON'
        AND o.po_number = '4755'
        AND m.order_id IS NULL
        GROUP BY o.order_type
        ORDER BY o.order_type
        """
        
        summary_df = pd.read_sql(unmatched_summary_query, conn)
        print(summary_df.to_string(index=False))
        
        print(f"\n💡 NEXT STEPS:")
        print(f"1. Investigate the {len(failed_df)} failed quantity matches manually")
        print(f"2. Consider Layer 3 matching: ACTIVE unmatched orders to resolve variances") 
        print(f"3. Consider Layer 4 matching: CANCELLED orders for historical context")
        print(f"4. Build manual linking interface for quantity variance resolution")

if __name__ == "__main__":
    analyze_quantity_variances()
