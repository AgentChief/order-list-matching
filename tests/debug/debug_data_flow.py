"""
Database Data Flow Debug Tool
Analyzes table structures, data flow, and identifies issues in the order matching system
"""

import argparse
import sys
from pathlib import Path
import pyodbc
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from auth_helper import get_connection_string

class DataFlowDebugger:
    def __init__(self):
        self.connection_string = get_connection_string()
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def debug01_table_inventory(self):
        """Debug 01: Inventory all tables and views in the system"""
        print("🔍 DEBUG 01: TABLE AND VIEW INVENTORY")
        print("=" * 60)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all tables and views with column counts
            cursor.execute("""
            SELECT 
                t.TABLE_NAME,
                t.TABLE_TYPE,
                COUNT(c.COLUMN_NAME) as column_count
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
            WHERE t.TABLE_SCHEMA = 'dbo'
                AND (
                    t.TABLE_NAME LIKE '%order%' OR 
                    t.TABLE_NAME LIKE '%ship%' OR
                    t.TABLE_NAME LIKE '%stg_%' OR
                    t.TABLE_NAME LIKE '%int_%' OR
                    t.TABLE_NAME LIKE '%mart_%' OR
                    t.TABLE_NAME LIKE '%enhanced%' OR
                    t.TABLE_NAME LIKE '%FM_%'
                )
            GROUP BY t.TABLE_NAME, t.TABLE_TYPE
            ORDER BY t.TABLE_TYPE DESC, t.TABLE_NAME
            """)
            
            print(f"{'TABLE/VIEW NAME':<40} | {'TYPE':<10} | {'COLUMNS'}")
            print("-" * 60)
            
            for row in cursor.fetchall():
                print(f"{row[0]:<40} | {row[1]:<10} | {row[2]}")
    
    def debug02_delivery_method_audit(self):
        """Debug 02: Audit delivery_method field across all relevant tables"""
        print("\n🚚 DEBUG 02: DELIVERY METHOD FIELD AUDIT")
        print("=" * 60)
        
        # Tables to check for delivery_method
        tables_to_check = [
            'FM_orders_shipped',
            'stg_fm_orders_shipped_table', 
            'stg_fm_orders_shipped',
            'int_shipments_extended',
            'ORDER_LIST',
            'stg_order_list',
            'int_orders_extended',
            'enhanced_matching_results'
        ]
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            print(f"{'TABLE NAME':<35} | {'HAS DELIVERY_METHOD':<20} | {'SAMPLE VALUE'}")
            print("-" * 80)
            
            for table in tables_to_check:
                try:
                    # Check if table exists and has delivery_method column
                    cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table}' 
                        AND COLUMN_NAME LIKE '%delivery%'
                    """)
                    
                    delivery_columns = [row[0] for row in cursor.fetchall()]
                    
                    if delivery_columns:
                        # Get sample delivery method values
                        for col in delivery_columns:
                            cursor.execute(f"""
                            SELECT TOP 1 [{col}]
                            FROM [{table}] 
                            WHERE [{col}] IS NOT NULL 
                                AND po_number = '4755'
                            """)
                            
                            result = cursor.fetchone()
                            sample_value = result[0] if result else "NO DATA FOR PO 4755"
                            print(f"{table:<35} | {col:<20} | {sample_value}")
                    else:
                        print(f"{table:<35} | {'NO DELIVERY COLUMNS':<20} | N/A")
                        
                except Exception as e:
                    print(f"{table:<35} | {'ERROR':<20} | {str(e)[:30]}...")
    
    def debug03_greyson_po4755_data_flow(self):
        """Debug 03: Trace GREYSON PO 4755 data through the entire pipeline"""
        print("\n📊 DEBUG 03: GREYSON PO 4755 DATA FLOW ANALYSIS")
        print("=" * 60)
        
        with self.get_connection() as conn:
            
            # Source: FM_orders_shipped
            print("\n--- SOURCE: FM_orders_shipped ---")
            try:
                df = pd.read_sql("""
                SELECT TOP 5
                    ID,
                    [Customer_PO] as po_number,
                    [CUSTOMER STYLE] as style_code,
                    [Customer_Colour_Description] as color_description,
                    [Shipping_Method] as delivery_method,
                    [Quantity_Shipped] as quantity
                FROM FM_orders_shipped 
                WHERE [Customer_PO] = '4755'
                """, conn)
                
                print(f"Records found: {len(df)}")
                if not df.empty:
                    print("Sample data:")
                    print(df.to_string(index=False))
                    print(f"Unique delivery methods: {df['delivery_method'].unique()}")
                
            except Exception as e:
                print(f"ERROR: {e}")
            
            # Staging: stg_fm_orders_shipped_table
            print("\n--- STAGING: stg_fm_orders_shipped_table ---")
            try:
                df = pd.read_sql("""
                SELECT TOP 5
                    shipment_id,
                    po_number,
                    style_code,
                    color_description,
                    delivery_method,
                    quantity
                FROM stg_fm_orders_shipped_table 
                WHERE po_number = '4755'
                """, conn)
                
                print(f"Records found: {len(df)}")
                if not df.empty:
                    print("Sample data:")
                    print(df.to_string(index=False))
                    print(f"Unique delivery methods: {df['delivery_method'].unique()}")
                
            except Exception as e:
                print(f"ERROR: {e}")
            
            # Enhanced Matching Results
            print("\n--- RESULTS: enhanced_matching_results ---")
            try:
                df = pd.read_sql("""
                SELECT TOP 5
                    shipment_id,
                    order_id,
                    shipment_delivery_method,
                    order_delivery_method,
                    delivery_match
                FROM enhanced_matching_results 
                WHERE po_number = '4755'
                """, conn)
                
                print(f"Records found: {len(df)}")
                if not df.empty:
                    print("Sample data:")
                    print(df.to_string(index=False))
                    print(f"Shipment delivery methods: {df['shipment_delivery_method'].unique()}")
                    print(f"Order delivery methods: {df['order_delivery_method'].unique()}")
                
            except Exception as e:
                print(f"ERROR: {e}")
    
    def debug04_enhanced_matcher_queries(self):
        """Debug 04: Test the exact queries used by enhanced matcher"""
        print("\n🔧 DEBUG 04: ENHANCED MATCHER QUERY TESTING")
        print("=" * 60)
        
        with self.get_connection() as conn:
            
            # Test orders query (from enhanced_db_matcher.py line 67-81)
            print("\n--- ORDERS QUERY (enhanced_db_matcher.py) ---")
            try:
                df = pd.read_sql("""
                SELECT 
                    order_id,
                    customer_name,
                    po_number,
                    delivery_method,
                    style_code,
                    color_description,
                    aag_order_number,
                    order_type,
                    order_date,
                    quantity,
                    CONCAT(style_code, '-', color_description) as style_color_key
                FROM stg_order_list 
                WHERE customer_name LIKE 'GREYSON%' AND po_number = '4755'
                """, conn)
                
                print(f"Records found: {len(df)}")
                if not df.empty:
                    print("Sample data:")
                    print(df[['order_id', 'style_code', 'color_description', 'delivery_method', 'quantity']].head())
                    print(f"Unique delivery methods: {df['delivery_method'].unique()}")
                
            except Exception as e:
                print(f"ERROR: {e}")
            
            # Test shipments query (from enhanced_db_matcher.py line 95-108) - MISSING delivery_method!
            print("\n--- SHIPMENTS QUERY (enhanced_db_matcher.py) - CURRENT VERSION ---")
            try:
                df = pd.read_sql("""
                SELECT 
                    shipment_id,
                    customer_name,
                    po_number,
                    style_code,
                    color_description,
                    quantity,
                    shipped_date,
                    style_color_key,
                    customer_po_key
                FROM stg_fm_orders_shipped_table
                WHERE customer_name LIKE 'GREYSON%' AND po_number = '4755'
                """, conn)
                
                print(f"Records found: {len(df)}")
                if not df.empty:
                    print("Sample data:")
                    print(df[['shipment_id', 'style_code', 'color_description', 'quantity']].head())
                    print("⚠️  NOTICE: delivery_method is MISSING from this query!")
                
            except Exception as e:
                print(f"ERROR: {e}")
            
            # Test corrected shipments query (what it SHOULD be)
            print("\n--- CORRECTED SHIPMENTS QUERY (what it SHOULD be) ---")
            try:
                df = pd.read_sql("""
                SELECT 
                    shipment_id,
                    customer_name,
                    po_number,
                    style_code,
                    color_description,
                    delivery_method,
                    quantity,
                    shipped_date,
                    style_color_key,
                    customer_po_key
                FROM stg_fm_orders_shipped_table
                WHERE customer_name LIKE 'GREYSON%' AND po_number = '4755'
                """, conn)
                
                print(f"Records found: {len(df)}")
                if not df.empty:
                    print("Sample data:")
                    print(df[['shipment_id', 'style_code', 'color_description', 'delivery_method', 'quantity']].head())
                    print(f"✅ Unique delivery methods: {df['delivery_method'].unique()}")
                
            except Exception as e:
                print(f"ERROR: {e}")
    
    def debug05_models_layer_usage(self):
        """Debug 05: Determine which models layer files are actually in use"""
        print("\n🏗️  DEBUG 05: MODELS LAYER USAGE ANALYSIS")
        print("=" * 60)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if views from models folder actually exist
            model_views = [
                'stg_fm_orders_shipped',
                'stg_order_list', 
                'int_orders_extended',
                'int_shipments_extended'
            ]
            
            print(f"{'MODEL VIEW':<30} | {'EXISTS':<10} | {'RECORDS':<10} | {'USED BY MATCHER'}")
            print("-" * 75)
            
            for view in model_views:
                try:
                    # Check if view exists
                    cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.VIEWS 
                    WHERE TABLE_NAME = '{view}'
                    """)
                    
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists:
                        # Check record count
                        cursor.execute(f"SELECT COUNT(*) FROM {view}")
                        record_count = cursor.fetchone()[0]
                        
                        # Check if used by enhanced matcher
                        used_by_matcher = "❌ NO" 
                        if view == 'stg_order_list':
                            used_by_matcher = "✅ YES (orders)"
                        elif view == 'stg_fm_orders_shipped_table':  # Not a view, but the table
                            used_by_matcher = "✅ YES (shipments)"
                        
                        print(f"{view:<30} | {'✅ YES':<10} | {record_count:<10} | {used_by_matcher}")
                    else:
                        print(f"{view:<30} | {'❌ NO':<10} | {'N/A':<10} | {'N/A'}")
                        
                except Exception as e:
                    print(f"{view:<30} | {'ERROR':<10} | {'N/A':<10} | {str(e)[:20]}...")
    
    def debug06_cleanup_recommendations(self):
        """Debug 06: Generate cleanup recommendations based on analysis"""
        print("\n🧹 DEBUG 06: CLEANUP RECOMMENDATIONS")
        print("=" * 60)
        
        print("""
FINDINGS SUMMARY:
================

1. CRITICAL ISSUE FOUND:
   ❌ enhanced_db_matcher.py shipments query is MISSING delivery_method field
   📍 Location: Line 95-108 in get_shipments_with_exclusions()
   🔧 Fix: Add 'delivery_method,' to the SELECT statement

2. DATA FLOW STRUCTURE:
   ✅ Source: FM_orders_shipped → Has delivery_method as 'Shipping_Method'
   ✅ Staging: stg_fm_orders_shipped_table → Has delivery_method field
   ❌ Matcher: Query missing delivery_method → Results in NULL/N/A values
   ❌ HITL UI: Shows blank delivery columns

3. MODELS LAYER STATUS:
   🔍 Need to verify which db/models/*.sql files are actually deployed as views
   🔍 Some may be redundant if not used by the application

RECOMMENDED CLEANUP PLAN:
========================

PHASE 1: IMMEDIATE FIXES
- Fix enhanced_db_matcher.py shipments query (add delivery_method)
- Rerun GREYSON PO 4755 matching to populate enhanced_matching_results correctly
- Verify HITL UI shows delivery methods properly

PHASE 2: STRUCTURE CLEANUP  
- Audit which db/models views are actually deployed and used
- Remove unused/redundant files
- Consolidate overlapping functionality
- Document the operational data flow

PHASE 3: DOCUMENTATION
- Create clear data flow diagram
- Document which tables/views are operational vs deprecated
- Update README with current architecture
        """)
    
    def debug07_hitl_interface_completeness(self):
        """Debug 07: Analyze HITL interface data completeness for GREYSON PO 4755"""
        print("\n🎯 DEBUG 07: HITL INTERFACE COMPLETENESS ANALYSIS")
        print("=" * 60)
        
        with self.get_connection() as conn:
            
            # 1. All shipments (source data)
            print("\n📦 ALL SHIPMENTS (Source Data)")
            try:
                df_shipments = pd.read_sql("""
                SELECT 
                    shipment_id,
                    style_code,
                    color_description,
                    delivery_method,
                    quantity
                FROM stg_fm_orders_shipped_table 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                ORDER BY shipment_id
                """, conn)
                
                print(f"Total shipments: {len(df_shipments)}")
                delivery_methods = sorted(df_shipments['delivery_method'].unique())
                print(f"Delivery methods: {delivery_methods}")
                print(f"Quantity range: {df_shipments['quantity'].min()} - {df_shipments['quantity'].max()}")
                
            except Exception as e:
                print(f"ERROR: {e}")
            
            # 2. All orders (source data) 
            print("\n📋 ALL ORDERS (Source Data)")
            try:
                df_orders = pd.read_sql("""
                SELECT 
                    order_id,
                    style_code,
                    color_description,
                    delivery_method,
                    quantity,
                    order_type
                FROM stg_order_list 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                ORDER BY order_id
                """, conn)
                
                print(f"Total orders: {len(df_orders)}")
                order_types = sorted(df_orders['order_type'].unique())
                order_delivery_methods = sorted(df_orders['delivery_method'].unique())
                print(f"Order types: {order_types}")
                print(f"Delivery methods: {order_delivery_methods}")
                print(f"Quantity range: {df_orders['quantity'].min()} - {df_orders['quantity'].max()}")
                
            except Exception as e:
                print(f"ERROR: {e}")
            
            # 3. Current matches in enhanced_matching_results
            print("\n✅ CURRENT MATCHES (enhanced_matching_results)")
            try:
                df_matches = pd.read_sql("""
                SELECT 
                    match_layer,
                    style_match,
                    color_match,
                    delivery_match,
                    quantity_check_result,
                    COUNT(*) as count
                FROM enhanced_matching_results 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                GROUP BY match_layer, style_match, color_match, delivery_match, quantity_check_result
                ORDER BY match_layer, count DESC
                """, conn)
                
                print("Matching breakdown:")
                print(df_matches.to_string(index=False))
                
                # Calculate totals
                total_matches = df_matches['count'].sum()
                layer0_matches = df_matches[df_matches['match_layer'] == 0]['count'].sum() if not df_matches[df_matches['match_layer'] == 0].empty else 0
                layer1_matches = df_matches[df_matches['match_layer'] == 1]['count'].sum() if not df_matches[df_matches['match_layer'] == 1].empty else 0
                
                print(f"\nTotal matches: {total_matches}")
                print(f"Layer 0 (exact): {layer0_matches}")
                print(f"Layer 1 (fuzzy): {layer1_matches}")
                
            except Exception as e:
                print(f"ERROR: {e}")
            
            # 4. Unmatched shipments analysis
            print("\n❌ UNMATCHED SHIPMENTS ANALYSIS")
            try:
                df_unmatched = pd.read_sql("""
                SELECT 
                    s.shipment_id,
                    s.style_code,
                    s.color_description,
                    s.delivery_method,
                    s.quantity,
                    'No match found' as reason
                FROM stg_fm_orders_shipped_table s
                LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                WHERE s.customer_name = 'GREYSON' 
                    AND s.po_number = '4755'
                    AND emr.shipment_id IS NULL
                ORDER BY s.shipment_id
                """, conn)
                
                print(f"Unmatched shipments: {len(df_unmatched)}")
                if not df_unmatched.empty:
                    print("Sample unmatched records:")
                    print(df_unmatched.head().to_string(index=False))
                
            except Exception as e:
                print(f"ERROR: {e}")
            
            # 5. Mismatch analysis by type
            print("\n🔍 MISMATCH ANALYSIS BY TYPE")
            try:
                # Style mismatches
                df_style_mismatches = pd.read_sql("""
                SELECT COUNT(*) as style_mismatches
                FROM enhanced_matching_results 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                    AND style_match = 'MISMATCH'
                """, conn)
                
                # Color mismatches  
                df_color_mismatches = pd.read_sql("""
                SELECT COUNT(*) as color_mismatches
                FROM enhanced_matching_results 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                    AND color_match = 'MISMATCH'
                """, conn)
                
                # Delivery mismatches
                df_delivery_mismatches = pd.read_sql("""
                SELECT COUNT(*) as delivery_mismatches
                FROM enhanced_matching_results 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                    AND delivery_match = 'MISMATCH'
                """, conn)
                
                # Quantity issues
                df_quantity_issues = pd.read_sql("""
                SELECT 
                    quantity_check_result,
                    COUNT(*) as count
                FROM enhanced_matching_results 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                GROUP BY quantity_check_result
                """, conn)
                
                print(f"Style mismatches: {df_style_mismatches.iloc[0]['style_mismatches']}")
                print(f"Color mismatches: {df_color_mismatches.iloc[0]['color_mismatches']}")
                print(f"Delivery mismatches: {df_delivery_mismatches.iloc[0]['delivery_mismatches']}")
                print("Quantity check results:")
                print(df_quantity_issues.to_string(index=False))
                
            except Exception as e:
                print(f"ERROR: {e}")
    
    def debug08_hitl_ui_recommendations(self):
        """Debug 08: Generate HITL UI improvement recommendations"""
        print("\n💡 DEBUG 08: HITL UI IMPROVEMENT RECOMMENDATIONS")
        print("=" * 60)
        
        print("""
CURRENT HITL INTERFACE GAPS:
============================

1. MISSING TABS/VIEWS:
   ❌ "All Shipments" tab - Users can't see complete shipment inventory (33 total)
   ❌ "Unmatched Shipments" tab - Users can't see what wasn't matched (10 records)
   ❌ "Layer 1 Matches" tab - Users can't distinguish fuzzy vs exact matches
   ❌ "Style Mismatches" tab - Currently shows no data but may exist
   ❌ "Color Mismatches" tab - Currently shows no data but may exist

2. INSUFFICIENT DATA CONTEXT:
   ❌ No visibility into WHY matches failed
   ❌ No ability to see match confidence scores
   ❌ No drill-down from summary to detail level
   ❌ No comparison view between matched pairs

3. NAVIGATION ISSUES:
   ❌ "All Matches" only shows Layer 0 - what about Layer 1?
   ❌ "Delivery Mismatches" incorrectly shows "No mismatches" 
   ❌ No path from overview to specific problem areas

RECOMMENDED HITL UI ENHANCEMENTS:
================================

TAB STRUCTURE REDESIGN:
----------------------
📊 Overview Dashboard
   - Total counts: Shipments (33), Orders (39), Matches (23), Unmatched (10)
   - Match success rate: 69.7%
   - Quick navigation to problem areas

📦 All Shipments (33 records)
   - Complete shipment inventory with match status
   - Filter by: Matched/Unmatched, Delivery Method, Style
   - Action: Navigate to specific match or mismatch details

✅ All Matches (23 records) 
   - Combined Layer 0 + Layer 1 matches
   - Tag matches by layer (Exact/Fuzzy)
   - Show confidence scores
   - Filter by match type, layer, confidence

❌ Unmatched Shipments (10 records)
   - Shipments with no corresponding orders
   - Analysis of why match failed
   - Suggested manual matching options

🔍 Mismatch Analysis
   - Style Mismatches: [count]
   - Color Mismatches: [count]  
   - Delivery Mismatches: [count]
   - Quantity Issues: [breakdown by type]

🎯 Match Details (drill-down)
   - Side-by-side comparison of matched pairs
   - Highlight differences
   - Confidence scoring explanation
   - Manual override options

IMPLEMENTATION PRIORITY:
=======================
1. HIGH: Add "All Shipments" tab for complete visibility
2. HIGH: Fix "Delivery Mismatches" showing incorrect data
3. MEDIUM: Add "Unmatched Shipments" analysis  
4. MEDIUM: Enhance "All Matches" to show Layer 0 vs Layer 1
5. LOW: Add detailed mismatch analysis tabs
        """)
            
        # Generate summary metrics for implementation
        with self.get_connection() as conn:
            print("\n📈 IMPLEMENTATION METRICS:")
            try:
                # Get exact counts for UI planning
                metrics = pd.read_sql("""
                SELECT 
                    'Total Shipments' as metric,
                    COUNT(*) as value
                FROM stg_fm_orders_shipped_table 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                
                UNION ALL
                
                SELECT 
                    'Total Orders' as metric,
                    COUNT(*) as value
                FROM stg_order_list 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                
                UNION ALL
                
                SELECT 
                    'Total Matches' as metric,
                    COUNT(*) as value
                FROM enhanced_matching_results 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                """, conn)
                
                print(metrics.to_string(index=False))
                
            except Exception as e:
                print(f"ERROR: {e}")
    
    def debug09_orders_investigation(self):
        """Debug 09: Investigate why orders query returns 0 records"""
        print("\n🔍 DEBUG 09: ORDERS QUERY INVESTIGATION")
        print("=" * 60)
        
        with self.get_connection() as conn:
            
            # 1. Check if stg_order_list table exists and has data
            print("\n1. Checking stg_order_list table existence and data...")
            try:
                df = pd.read_sql("SELECT COUNT(*) as total_count FROM stg_order_list", conn)
                total_count = df.iloc[0]['total_count']
                print(f"✅ Total records in stg_order_list: {total_count}")
            except Exception as e:
                print(f"❌ ERROR accessing stg_order_list: {e}")
                return
            
            # 2. Check for GREYSON data specifically
            print("\n2. Checking GREYSON customer data...")
            try:
                df = pd.read_sql("""
                SELECT 
                    customer_name,
                    COUNT(*) as count
                FROM stg_order_list 
                WHERE customer_name LIKE '%GREYSON%'
                GROUP BY customer_name
                ORDER BY count DESC
                """, conn)
                
                if len(df) > 0:
                    print("✅ GREYSON customer names found:")
                    print(df.to_string(index=False))
                else:
                    print("❌ No GREYSON customer data found")
                    
                    # Check what customer names DO exist
                    df_all = pd.read_sql("""
                    SELECT 
                        customer_name,
                        COUNT(*) as count
                    FROM stg_order_list 
                    GROUP BY customer_name
                    ORDER BY count DESC
                    """, conn)
                    print(f"\n📋 Available customer names (top 10):")
                    print(df_all.head(10).to_string(index=False))
                    
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 3. Check PO 4755 specifically across all customers
            print("\n3. Checking PO 4755 across all customers...")
            try:
                df = pd.read_sql("""
                SELECT 
                    customer_name,
                    po_number,
                    COUNT(*) as count
                FROM stg_order_list 
                WHERE po_number = '4755'
                GROUP BY customer_name, po_number
                ORDER BY count DESC
                """, conn)
                
                if len(df) > 0:
                    print("✅ PO 4755 found for customers:")
                    print(df.to_string(index=False))
                else:
                    print("❌ No PO 4755 found in any customer data")
                    
                    # Check what PO numbers exist for GREYSON-like customers
                    df_pos = pd.read_sql("""
                    SELECT 
                        po_number,
                        COUNT(*) as count
                    FROM stg_order_list 
                    WHERE customer_name LIKE '%GREY%'
                    GROUP BY po_number
                    ORDER BY count DESC
                    """, conn)
                    print(f"\n📋 Available PO numbers for GREY* customers:")
                    print(df_pos.head(10).to_string(index=False))
                    
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 4. Test the exact debug07 query with detailed diagnosis
            print("\n4. Testing exact debug07 query...")
            try:
                df = pd.read_sql("""
                SELECT 
                    order_id,
                    customer_name,
                    po_number,
                    style_code,
                    color_description,
                    delivery_method,
                    quantity,
                    order_type
                FROM stg_order_list 
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                ORDER BY order_id
                """, conn)
                
                print(f"🎯 Debug07 query result: {len(df)} records")
                if len(df) > 0:
                    print("✅ Sample data:")
                    print(df.head().to_string(index=False))
                else:
                    print("❌ No records returned - investigating alternatives...")
                    
                    # Try partial matches
                    df_partial = pd.read_sql("""
                    SELECT 
                        customer_name,
                        po_number,
                        COUNT(*) as count
                    FROM stg_order_list 
                    WHERE (customer_name LIKE '%GREY%' OR po_number LIKE '%4755%')
                    GROUP BY customer_name, po_number
                    ORDER BY count DESC
                    """, conn)
                    print("🔍 Partial matches found:")
                    print(df_partial.to_string(index=False))
                    
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 5. Compare with enhanced_matching_results to see the disconnect
            print("\n5. Comparing with enhanced_matching_results...")
            try:
                df = pd.read_sql("""
                SELECT DISTINCT
                    customer_name,
                    po_number,
                    COUNT(*) as match_count
                FROM enhanced_matching_results
                GROUP BY customer_name, po_number
                ORDER BY match_count DESC
                """, conn)
                
                print("✅ Customer/PO combinations in enhanced_matching_results:")
                print(df.to_string(index=False))
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
    
    def debug10_test_all_shipments_query(self):
        """Debug 10: Test and fix the All Shipments query"""
        print("\n🚢 DEBUG 10: ALL SHIPMENTS QUERY TESTING & FIXING")
        print("=" * 60)
        
        with self.get_connection() as conn:
            
            # 1. Test basic shipments query without parameters first
            print("\n1. Testing basic shipments query...")
            try:
                df = pd.read_sql("""
                SELECT 
                    shipment_id,
                    customer_name,
                    po_number,
                    style_code,
                    color_description,
                    delivery_method,
                    quantity,
                    shipped_date
                FROM stg_fm_orders_shipped_table
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                ORDER BY shipment_id
                """, conn)
                
                print(f"✅ Basic query result: {len(df)} records")
                if len(df) > 0:
                    print("Sample data:")
                    print(df.head(3).to_string(index=False))
                else:
                    print("❌ No records found with exact 'GREYSON' - trying LIKE pattern...")
                    
                    # Try with LIKE pattern
                    df2 = pd.read_sql("""
                    SELECT 
                        shipment_id,
                        customer_name,
                        po_number,
                        style_code,
                        color_description,
                        delivery_method,
                        quantity,
                        shipped_date
                    FROM stg_fm_orders_shipped_table
                    WHERE customer_name LIKE 'GREYSON%' AND po_number = '4755'
                    ORDER BY shipment_id
                    """, conn)
                    
                    print(f"✅ LIKE pattern result: {len(df2)} records")
                    if len(df2) > 0:
                        print("Sample data:")
                        print(df2.head(3).to_string(index=False))
                        df = df2  # Use this result for further testing
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
                return
            
            # 2. Test LEFT JOIN with enhanced_matching_results
            print("\n2. Testing LEFT JOIN with enhanced_matching_results...")
            try:
                df_join = pd.read_sql("""
                SELECT 
                    s.shipment_id,
                    s.style_code,
                    s.color_description,
                    s.delivery_method,
                    s.quantity,
                    s.shipped_date,
                    CASE 
                        WHEN emr.shipment_id IS NOT NULL THEN 'MATCHED'
                        ELSE 'UNMATCHED'
                    END as match_status,
                    emr.match_layer,
                    emr.match_confidence,
                    emr.style_match,
                    emr.color_match,
                    emr.delivery_match,
                    emr.quantity_check_result
                FROM stg_fm_orders_shipped_table s
                LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                WHERE s.customer_name LIKE 'GREYSON%' AND s.po_number = '4755'
                ORDER BY s.shipment_id
                """, conn)
                
                print(f"✅ JOIN query result: {len(df_join)} records")
                matched_count = len(df_join[df_join['match_status'] == 'MATCHED'])
                unmatched_count = len(df_join[df_join['match_status'] == 'UNMATCHED'])
                
                print(f"   - Matched: {matched_count}")
                print(f"   - Unmatched: {unmatched_count}")
                
                if len(df_join) > 0:
                    print("Sample data:")
                    print(df_join[['shipment_id', 'style_code', 'match_status', 'match_layer']].head(5).to_string(index=False))
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 3. Test correct parameterized query (fixed version)
            print("\n3. Testing CORRECTED parameterized query...")
            try:
                # Fixed query with proper parameter marker
                fixed_query = """
                SELECT 
                    s.shipment_id,
                    s.style_code,
                    s.color_description,
                    s.delivery_method,
                    s.quantity,
                    s.shipped_date,
                    CASE 
                        WHEN emr.shipment_id IS NOT NULL THEN 'MATCHED'
                        ELSE 'UNMATCHED'
                    END as match_status,
                    emr.match_layer,
                    emr.match_confidence,
                    emr.style_match,
                    emr.color_match,
                    emr.delivery_match,
                    emr.quantity_check_result
                FROM stg_fm_orders_shipped_table s
                LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                WHERE s.customer_name LIKE ? AND s.po_number = '4755'
                ORDER BY s.shipment_id
                """
                
                df_param = pd.read_sql(fixed_query, conn, params=['GREYSON%'])
                
                print(f"✅ FIXED parameterized query result: {len(df_param)} records")
                matched_count = len(df_param[df_param['match_status'] == 'MATCHED'])
                unmatched_count = len(df_param[df_param['match_status'] == 'UNMATCHED'])
                
                print(f"   - Matched: {matched_count}")
                print(f"   - Unmatched: {unmatched_count}")
                
                if len(df_param) > 0:
                    print("Sample data:")
                    print(df_param[['shipment_id', 'style_code', 'match_status', 'delivery_method']].head(5).to_string(index=False))
                
                print(f"\n🔧 SOLUTION IDENTIFIED:")
                print(f"   ❌ BROKEN: WHERE s.customer_name LIKE %s (wrong parameter marker)")
                print(f"   ✅ FIXED:  WHERE s.customer_name LIKE ? (correct parameter marker)")
                
            except Exception as e:
                print(f"❌ ERROR: {e}")

    def debug11_layer_matching_analysis(self):
        """Debug 11: Analyze layer matching attempts and unmatched records"""
        print("\n🎯 DEBUG 11: LAYER MATCHING & UNMATCHED RECORDS ANALYSIS")
        print("=" * 70)
        
        with self.get_connection() as conn:
            
            # 1. Check what layers exist in enhanced_matching_results
            print("\n1. Analyzing matching layers in enhanced_matching_results...")
            try:
                df_layers = pd.read_sql("""
                SELECT 
                    match_layer,
                    COUNT(*) as count,
                    AVG(match_confidence) as avg_confidence
                FROM enhanced_matching_results
                WHERE customer_name = 'GREYSON' AND po_number = '4755'
                GROUP BY match_layer
                ORDER BY match_layer
                """, conn)
                
                print(f"✅ Matching layers found:")
                print(df_layers.to_string(index=False))
                
                # Check if we have Layer 1 attempts
                layer1_count = df_layers[df_layers['match_layer'] == 'LAYER_1']['count'].sum() if 'LAYER_1' in df_layers['match_layer'].values else 0
                print(f"\n🔍 Layer 1 matches: {layer1_count}")
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 2. Get all orders that didn't match any shipment
            print("\n2. Finding orders from PO 4755 that didn't match any shipment...")
            try:
                df_unmatched_orders = pd.read_sql("""
                SELECT 
                    o.order_id,
                    o.customer_name,
                    o.po_number,
                    o.style_code,
                    o.color_description,
                    o.delivery_method,
                    o.quantity,
                    o.order_type
                FROM stg_order_list o
                WHERE o.customer_name LIKE 'GREYSON%' 
                    AND o.po_number = '4755'
                    AND NOT EXISTS (
                        SELECT 1 FROM enhanced_matching_results emr 
                        WHERE emr.order_id = o.order_id
                    )
                ORDER BY o.style_code, o.color_description
                """, conn)
                
                print(f"✅ Unmatched orders found: {len(df_unmatched_orders)}")
                if len(df_unmatched_orders) > 0:
                    print("Sample unmatched orders:")
                    print(df_unmatched_orders[['order_id', 'style_code', 'color_description', 'delivery_method', 'quantity']].to_string(index=False))
                    
                    # Group by style/color to see patterns
                    print("\n📊 Unmatched orders by style/color:")
                    style_color_summary = df_unmatched_orders.groupby(['style_code', 'color_description']).agg({
                        'quantity': 'sum',
                        'order_id': 'count'
                    }).rename(columns={'order_id': 'order_count'}).reset_index()
                    print(style_color_summary.to_string(index=False))
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 3. Analyze the 10 unmatched shipments in detail
            print("\n3. Analyzing the 10 unmatched shipments in detail...")
            try:
                df_unmatched_shipments = pd.read_sql("""
                SELECT 
                    s.shipment_id,
                    s.style_code,
                    s.color_description,
                    s.delivery_method,
                    s.quantity,
                    s.shipped_date
                FROM stg_fm_orders_shipped_table s
                WHERE s.customer_name = 'GREYSON' 
                    AND s.po_number = '4755'
                    AND NOT EXISTS (
                        SELECT 1 FROM enhanced_matching_results emr 
                        WHERE emr.shipment_id = s.shipment_id
                    )
                ORDER BY s.style_code, s.color_description
                """, conn)
                
                print(f"✅ Unmatched shipments: {len(df_unmatched_shipments)}")
                if len(df_unmatched_shipments) > 0:
                    print("Unmatched shipments details:")
                    print(df_unmatched_shipments.to_string(index=False))
                    
                    # Check if any orders exist with similar style/color
                    print("\n🔍 Checking for potential order matches for unmatched shipments...")
                    for _, shipment in df_unmatched_shipments.iterrows():
                        style = shipment['style_code']
                        color = shipment['color_description']
                        delivery = shipment['delivery_method']
                        
                        # Look for orders with same style/color but different delivery
                        df_potential = pd.read_sql("""
                        SELECT 
                            order_id,
                            style_code,
                            color_description,
                            delivery_method,
                            quantity
                        FROM stg_order_list
                        WHERE customer_name LIKE 'GREYSON%' 
                            AND po_number = '4755'
                            AND style_code = ?
                            AND color_description = ?
                        """, conn, params=[style, color])
                        
                        if len(df_potential) > 0:
                            print(f"\n📋 Shipment {shipment['shipment_id']} ({style} - {color} - {delivery}):")
                            print(f"   Potential order matches:")
                            print(df_potential[['order_id', 'delivery_method', 'quantity']].to_string(index=False))
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 4. Check delivery method impact on matching
            print("\n4. Analyzing delivery method impact on matching...")
            try:
                df_delivery_analysis = pd.read_sql("""
                SELECT 
                    s.delivery_method as shipment_delivery,
                    COUNT(*) as total_shipments,
                    COUNT(emr.shipment_id) as matched_shipments,
                    COUNT(*) - COUNT(emr.shipment_id) as unmatched_shipments,
                    ROUND(CAST(COUNT(emr.shipment_id) AS FLOAT) / COUNT(*) * 100, 1) as match_rate
                FROM stg_fm_orders_shipped_table s
                LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                WHERE s.customer_name = 'GREYSON' AND s.po_number = '4755'
                GROUP BY s.delivery_method
                ORDER BY unmatched_shipments DESC
                """, conn)
                
                print("✅ Delivery method matching analysis:")
                print(df_delivery_analysis.to_string(index=False))
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 5. Check if we're doing fuzzy matching on style/color
            print("\n5. Checking style/color matching patterns...")
            try:
                df_match_patterns = pd.read_sql("""
                SELECT 
                    emr.match_layer,
                    emr.style_match,
                    emr.color_match,
                    emr.delivery_match,
                    COUNT(*) as count
                FROM enhanced_matching_results emr
                WHERE emr.customer_name = 'GREYSON' AND emr.po_number = '4755'
                GROUP BY emr.match_layer, emr.style_match, emr.color_match, emr.delivery_match
                ORDER BY emr.match_layer, count DESC
                """, conn)
                
                print("✅ Matching patterns by layer:")
                print(df_match_patterns.to_string(index=False))
                
                # Check if we have any fuzzy matches
                fuzzy_matches = df_match_patterns[
                    (df_match_patterns['style_match'] == 'FUZZY') | 
                    (df_match_patterns['color_match'] == 'FUZZY')
                ]
                
                if len(fuzzy_matches) > 0:
                    print(f"\n🎯 Fuzzy matches found: {len(fuzzy_matches)} patterns")
                else:
                    print(f"\n⚠️  No fuzzy matches found - only exact matches")
                
            except Exception as e:
                print(f"❌ ERROR: {e}")

    def debug12_quantity_variance_analysis(self):
        """Debug 12: CRITICAL - Analyze quantity variances and find unmatched orders for investigation"""
        print("\n💰 DEBUG 12: QUANTITY VARIANCE ANALYSIS (THE REAL PROBLEM)")
        print("=" * 70)
        
        with self.get_connection() as conn:
            
            # 1. Get all matches with quantity variance details
            print("\n1. Analyzing quantity variances in matched shipments...")
            try:
                # First check available columns
                schema_check = pd.read_sql("SELECT TOP 1 * FROM enhanced_matching_results WHERE customer_name = 'GREYSON' AND po_number = '4755'", conn)
                print(f"📋 Available columns: {schema_check.columns.tolist()}")
                
                # Get detailed FAIL cases with quantity info
                df_variances_detail = pd.read_sql("""
                SELECT 
                    emr.shipment_id,
                    emr.order_id,
                    emr.shipment_style_code,
                    emr.shipment_color_description,
                    emr.shipment_quantity,
                    emr.order_quantity,
                    emr.quantity_difference_percent,
                    emr.quantity_check_result,
                    emr.shipment_delivery_method,
                    emr.order_delivery_method,
                    emr.delivery_match
                FROM enhanced_matching_results emr
                WHERE emr.customer_name = 'GREYSON' 
                AND emr.po_number = '4755'
                AND emr.quantity_check_result = 'FAIL'
                ORDER BY ABS(emr.quantity_difference_percent) DESC
                """, conn)
                
                print(f"🚨 FAILED quantity matches: {len(df_variances_detail)}")
                if len(df_variances_detail) > 0:
                    print(f"📊 Failed matches with details:")
                    print(df_variances_detail[['shipment_id', 'shipment_style_code', 'shipment_color_description', 
                                              'shipment_quantity', 'order_quantity', 'quantity_difference_percent']].to_string(index=False))
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 2. For each failed quantity match, find unmatched ACTIVE orders with same style/color
            print(f"\n2. Finding unmatched ACTIVE orders that could explain quantity gaps...")
            try:
                df_potential_matches = pd.read_sql("""
                -- For each FAILED quantity match, find unmatched ACTIVE orders with same style/color
                WITH failed_matches AS (
                    SELECT DISTINCT
                        emr.shipment_style_code,
                        emr.shipment_color_description,
                        emr.shipment_quantity,
                        SUM(emr.order_quantity) as total_matched_qty,
                        emr.shipment_quantity - SUM(emr.order_quantity) as qty_gap
                    FROM enhanced_matching_results emr
                    WHERE emr.customer_name = 'GREYSON' 
                    AND emr.po_number = '4755'
                    AND emr.quantity_check_result = 'FAIL'
                    GROUP BY emr.shipment_style_code, emr.shipment_color_description, emr.shipment_quantity
                ),
                unmatched_active_orders AS (
                    SELECT 
                        ol.order_id,
                        ol.style_code,
                        ol.color_description,
                        ol.quantity,
                        ol.delivery_method,
                        ol.order_type
                    FROM stg_order_list ol
                    LEFT JOIN enhanced_matching_results emr ON ol.order_id = emr.order_id
                    WHERE ol.customer_name = 'GREYSON'
                    AND ol.po_number = '4755'
                    AND ol.order_type = 'ACTIVE'
                    AND ol.quantity > 0
                    AND emr.order_id IS NULL  -- Not matched
                )
                SELECT 
                    fm.shipment_style_code,
                    fm.shipment_color_description,
                    fm.shipment_quantity,
                    fm.total_matched_qty,
                    fm.qty_gap,
                    uao.order_id,
                    uao.quantity as unmatched_order_qty,
                    uao.delivery_method as unmatched_delivery,
                    CASE 
                        WHEN ABS(fm.qty_gap - uao.quantity) <= fm.qty_gap * 0.1 THEN 'POTENTIAL_MATCH'
                        ELSE 'PARTIAL_MATCH'
                    END as match_potential
                FROM failed_matches fm
                INNER JOIN unmatched_active_orders uao 
                    ON fm.shipment_style_code = uao.style_code 
                    AND fm.shipment_color_description = uao.color_description
                ORDER BY fm.shipment_style_code, fm.qty_gap DESC, uao.quantity DESC
                """, conn)
                
                print(f"🎯 Potential order matches for quantity gaps: {len(df_potential_matches)}")
                if len(df_potential_matches) > 0:
                    print(df_potential_matches.to_string(index=False))
                else:
                    print("❌ No unmatched ACTIVE orders found with matching style/color")
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 3. Check CANCELLED orders that might explain the variances
            print(f"\n3. Checking CANCELLED orders that might explain quantity variances...")
            try:
                df_cancelled_analysis = pd.read_sql("""
                -- Find CANCELLED orders with same style/color as failed matches
                WITH failed_matches AS (
                    SELECT DISTINCT
                        emr.shipment_style_code,
                        emr.shipment_color_description,
                        emr.shipment_quantity - SUM(emr.order_quantity) as qty_gap
                    FROM enhanced_matching_results emr
                    WHERE emr.customer_name = 'GREYSON' 
                    AND emr.po_number = '4755'
                    AND emr.quantity_check_result = 'FAIL'
                    GROUP BY emr.shipment_style_code, emr.shipment_color_description, emr.shipment_quantity
                )
                SELECT 
                    fm.shipment_style_code,
                    fm.shipment_color_description,
                    fm.qty_gap,
                    ol.order_id,
                    ol.quantity as cancelled_qty,
                    ol.delivery_method,
                    ol.order_date,
                    CASE 
                        WHEN ol.quantity >= ABS(fm.qty_gap) * 0.8 THEN 'EXPLAINS_VARIANCE'
                        ELSE 'PARTIAL_EXPLANATION'
                    END as explanation_level
                FROM failed_matches fm
                INNER JOIN stg_order_list ol 
                    ON fm.shipment_style_code = ol.style_code 
                    AND fm.shipment_color_description = ol.color_description
                WHERE ol.customer_name = 'GREYSON'
                AND ol.po_number = '4755'
                AND ol.order_type = 'CANCELLED'
                ORDER BY fm.shipment_style_code, ol.quantity DESC
                """, conn)
                
                print(f"🚫 CANCELLED orders that might explain variances: {len(df_cancelled_analysis)}")
                if len(df_cancelled_analysis) > 0:
                    print(df_cancelled_analysis.to_string(index=False))
                else:
                    print("❌ No CANCELLED orders found with matching style/color")
                
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
            # 4. Summary recommendations
            print(f"\n4. QUANTITY VARIANCE RESOLUTION RECOMMENDATIONS:")
            print("=" * 60)
            print("""
IMMEDIATE ACTIONS FOR USER:
==========================

1. 🚨 CRITICAL: Review the 13 FAILED quantity matches above
   - These represent shipments with >10% quantity variance from orders
   - Each needs manual investigation

2. 🎯 INVESTIGATE: For each failed match, check:
   - Unmatched ACTIVE orders with same style/color (potential missing links)
   - CANCELLED orders that might explain the original quantity difference
   - Split shipments that weren't properly consolidated

3. 🔧 PROPOSED LAYER 3/4 MATCHING:
   - Layer 3: Auto-match unmatched ACTIVE orders to resolve quantity gaps
   - Layer 4: Link CANCELLED orders to explain historical quantity changes

4. 💡 HITL UI ENHANCEMENT:
   - Show quantity variance details prominently
   - For each variance, show potential unmatched orders for manual linking
   - Allow users to link additional orders to resolve quantity gaps
            """)

    def debug13_test_failing_shipment_summary_query(self):
        """Debug 13: Test the exact failing shipment summary query from streamlit app"""
        print("\n🚨 DEBUG 13: SHIPMENT SUMMARY QUERY FAILURE ANALYSIS")
        print("=" * 70)
        
        with self.get_connection() as conn:
            
            # 1. Test the BROKEN query (exact copy from error message)
            print("\n1. Testing the BROKEN query (exact from error message)...")
            try:
                broken_query = """
                SELECT 
                    s.shipment_id, 
                    s.style_code as shipment_style, 
                    s.color_description as shipment_color, 
                    s.delivery_method as shipment_delivery, 
                    s.quantity as shipment_quantity, 
                    COUNT(emr.id) as match_count, 
                    CASE 
                        WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 'QUANTITY_ISSUES' 
                        WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 'DELIVERY_ISSUES' 
                        ELSE 'GOOD' 
                    END as shipment_status, 
                    MIN(emr.match_layer) as best_layer, 
                    MAX(emr.match_confidence) as best_confidence 
                FROM stg_fm_orders_shipped_table s 
                INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                GROUP BY s.shipment_id, s.style_code, s.color_description, s.delivery_method, s.quantity
                ORDER BY 
                    CASE shipment_status 
                        WHEN 'QUANTITY_ISSUES' THEN 1
                        WHEN 'DELIVERY_ISSUES' THEN 2  
                        WHEN 'GOOD' THEN 3
                    END,
                    s.shipment_id
                """
                
                df = pd.read_sql(broken_query, conn)
                print(f"❌ UNEXPECTED: Broken query worked! {len(df)} records")
                
            except Exception as e:
                print(f"✅ EXPECTED FAILURE: {e}")
                print("   🔍 Problem: Cannot reference column alias 'shipment_status' in ORDER BY")
            
            # 2. Test the FIXED query (repeat CASE expression in ORDER BY)
            print("\n2. Testing the FIXED query (repeat CASE expression)...")
            try:
                fixed_query = """
                SELECT 
                    s.shipment_id, 
                    s.style_code as shipment_style, 
                    s.color_description as shipment_color, 
                    s.delivery_method as shipment_delivery, 
                    s.quantity as shipment_quantity, 
                    COUNT(emr.id) as match_count, 
                    CASE 
                        WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 'QUANTITY_ISSUES' 
                        WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 'DELIVERY_ISSUES' 
                        ELSE 'GOOD' 
                    END as shipment_status, 
                    MIN(emr.match_layer) as best_layer, 
                    MAX(emr.match_confidence) as best_confidence 
                FROM stg_fm_orders_shipped_table s 
                INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                WHERE s.customer_name LIKE ?
                GROUP BY s.shipment_id, s.style_code, s.color_description, s.delivery_method, s.quantity
                ORDER BY 
                    CASE 
                        WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 1
                        WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 2  
                        ELSE 3
                    END,
                    s.shipment_id
                """
                
                df = pd.read_sql(fixed_query, conn, params=['GREYSON%'])
                print(f"✅ FIXED QUERY SUCCESS: {len(df)} records")
                
                if len(df) > 0:
                    print("Sample results:")
                    print(df[['shipment_id', 'shipment_style', 'shipment_status', 'match_count']].head().to_string(index=False))
                    
                    # Check status distribution
                    status_counts = df['shipment_status'].value_counts()
                    print(f"\n📊 Status distribution:")
                    print(status_counts.to_string())
                
            except Exception as e:
                print(f"❌ FIXED QUERY FAILED: {e}")
                
            # 3. Test simpler alternative (use column position in ORDER BY)
            print("\n3. Testing ALTERNATIVE SOLUTION (column position in ORDER BY)...")
            try:
                alt_query = """
                SELECT 
                    s.shipment_id, 
                    s.style_code as shipment_style, 
                    s.color_description as shipment_color, 
                    s.delivery_method as shipment_delivery, 
                    s.quantity as shipment_quantity, 
                    COUNT(emr.id) as match_count, 
                    CASE 
                        WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 'QUANTITY_ISSUES' 
                        WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 'DELIVERY_ISSUES' 
                        ELSE 'GOOD' 
                    END as shipment_status, 
                    MIN(emr.match_layer) as best_layer, 
                    MAX(emr.match_confidence) as best_confidence 
                FROM stg_fm_orders_shipped_table s 
                INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                WHERE s.customer_name LIKE ?
                GROUP BY s.shipment_id, s.style_code, s.color_description, s.delivery_method, s.quantity
                ORDER BY 7, s.shipment_id
                """
                
                df = pd.read_sql(alt_query, conn, params=['GREYSON%'])
                print(f"✅ ALTERNATIVE SUCCESS: {len(df)} records")
                print("   💡 Using column position 7 (shipment_status) in ORDER BY")
                
            except Exception as e:
                print(f"❌ ALTERNATIVE FAILED: {e}")
                
            # 4. Generate the corrected code
            print(f"\n4. 🔧 SOLUTION FOR STREAMLIT APP:")
            print("=" * 50)
            print("PROBLEM: SQL Server cannot reference column aliases in ORDER BY with aggregate functions")
            print("SOLUTION: Repeat the CASE expression in ORDER BY clause")
            print()
            print("✅ Replace the ORDER BY clause in get_shipment_level_summary() with:")
            print("""
ORDER BY 
    CASE 
        WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 1
        WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 2  
        ELSE 3
    END,
    s.shipment_id
            """)

    def debug14_enhanced_shipment_summary_design(self):
        """Debug 14: Design enhanced shipment summary with row numbers, match indicators, confidence, and consolidated layers"""
        print("\n📊 DEBUG 14: ENHANCED SHIPMENT SUMMARY DESIGN")
        print("=" * 70)
        
        with self.get_connection() as conn:
            
            # 1. Test enhanced query with all requested features
            print("\n1. Testing ENHANCED shipment summary query...")
            try:
                enhanced_query = """
                WITH shipment_summary AS (
                    SELECT 
                        s.shipment_id,
                        s.style_code as shipment_style,
                        s.color_description as shipment_color,
                        s.delivery_method as shipment_delivery,
                        s.quantity as shipment_quantity,
                        COUNT(emr.id) as match_count,
                        
                        -- Match status indicators (tick/cross)
                        CASE 
                            WHEN COUNT(CASE WHEN emr.style_match = 'EXACT' THEN 1 END) > 0 THEN '✓'
                            WHEN COUNT(CASE WHEN emr.style_match = 'FUZZY' THEN 1 END) > 0 THEN '~'
                            ELSE '✗'
                        END as style_match_indicator,
                        
                        CASE 
                            WHEN COUNT(CASE WHEN emr.color_match = 'EXACT' THEN 1 END) > 0 THEN '✓'
                            WHEN COUNT(CASE WHEN emr.color_match = 'FUZZY' THEN 1 END) > 0 THEN '~'
                            ELSE '✗'
                        END as color_match_indicator,
                        
                        CASE 
                            WHEN COUNT(CASE WHEN emr.delivery_match = 'EXACT' THEN 1 END) > 0 THEN '✓'
                            ELSE '✗'
                        END as delivery_match_indicator,
                        
                        -- Consolidated layer information
                        STRING_AGG(DISTINCT emr.match_layer, ',') as match_layers,
                        
                        -- Confidence levels
                        MAX(emr.match_confidence) as best_confidence,
                        AVG(emr.match_confidence) as avg_confidence,
                        
                        -- Matched order quantities (total)
                        SUM(emr.order_quantity) as total_matched_order_qty,
                        
                        -- Quantity variance
                        CASE 
                            WHEN s.quantity - SUM(emr.order_quantity) = 0 THEN '✓'
                            WHEN ABS(s.quantity - SUM(emr.order_quantity)) <= s.quantity * 0.1 THEN '~'
                            ELSE '✗'
                        END as quantity_match_indicator,
                        
                        s.quantity - SUM(emr.order_quantity) as quantity_variance,
                        
                        -- Overall status
                        CASE 
                            WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 'QUANTITY_ISSUES'
                            WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 'DELIVERY_ISSUES'
                            ELSE 'GOOD'
                        END as shipment_status,
                        
                        -- Outstanding reviews count (placeholder - need to check if review table exists)
                        0 as outstanding_reviews
                        
                    FROM stg_fm_orders_shipped_table s
                    INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                    WHERE s.customer_name LIKE ?
                    GROUP BY s.shipment_id, s.style_code, s.color_description, s.delivery_method, s.quantity
                )
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY 
                        CASE shipment_status 
                            WHEN 'QUANTITY_ISSUES' THEN 1
                            WHEN 'DELIVERY_ISSUES' THEN 2  
                            WHEN 'GOOD' THEN 3
                        END,
                        shipment_id
                    ) as row_num,
                    *
                FROM shipment_summary
                """
                
                df = pd.read_sql(enhanced_query, conn, params=['GREYSON%'])
                print(f"✅ ENHANCED QUERY SUCCESS: {len(df)} records")
                
                if len(df) > 0:
                    print("\nSample enhanced results:")
                    display_cols = ['row_num', 'shipment_id', 'shipment_style', 'style_match_indicator', 
                                   'color_match_indicator', 'delivery_match_indicator', 'match_layers', 
                                   'best_confidence', 'quantity_match_indicator', 'shipment_status']
                    print(df[display_cols].head().to_string(index=False))
                    
                    print(f"\n📊 Enhanced metrics:")
                    print(f"   - Status distribution: {dict(df['shipment_status'].value_counts())}")
                    print(f"   - Match layers found: {sorted(set(','.join(df['match_layers'].dropna()).split(',')))}")
                    print(f"   - Confidence range: {df['best_confidence'].min():.1f} - {df['best_confidence'].max():.1f}")
                
            except Exception as e:
                print(f"❌ ENHANCED QUERY FAILED: {e}")
                print("   🔍 SQL Issue: STRING_AGG or CTE syntax problem")
                
                # Try simplified version without CTE
                print("\n1b. Testing SIMPLIFIED enhanced query (without CTE)...")
                try:
                    simplified_query = """
                    SELECT 
                        ROW_NUMBER() OVER (ORDER BY 
                            CASE 
                                WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 1
                                WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 2  
                                ELSE 3
                            END,
                            s.shipment_id
                        ) as row_num,
                        s.shipment_id,
                        s.style_code as shipment_style,
                        s.color_description as shipment_color,
                        s.delivery_method as shipment_delivery,
                        s.quantity as shipment_quantity,
                        COUNT(emr.id) as match_count,
                        
                        -- Match status indicators (using actual values from pattern analysis - fixed delivery logic)
                        CASE 
                            WHEN MAX(CASE WHEN emr.style_match = 'MATCH' THEN 1 ELSE 0 END) = 1 THEN 'Y'
                            ELSE 'N'
                        END as style_match_indicator,
                        
                        CASE 
                            WHEN MAX(CASE WHEN emr.color_match = 'MATCH' THEN 1 ELSE 0 END) = 1 THEN 'Y'
                            ELSE 'N'
                        END as color_match_indicator,
                        
                        CASE 
                            WHEN MAX(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 ELSE 0 END) = 1 THEN 'N'
                            WHEN MAX(CASE WHEN emr.delivery_match = 'MATCH' THEN 1 ELSE 0 END) = 1 THEN 'Y'
                            ELSE 'U'
                        END as delivery_match_indicator,
                        
                        -- Simplified layer consolidation (just min/max for now)
                        MIN(emr.match_layer) + '-' + MAX(emr.match_layer) as match_layers,
                        
                        -- Confidence levels
                        MAX(emr.match_confidence) as best_confidence,
                        AVG(emr.match_confidence) as avg_confidence,
                        
                        -- Matched order quantities (total)
                        SUM(emr.order_quantity) as total_matched_order_qty,
                        
                        -- Quantity variance
                        CASE 
                            WHEN s.quantity - SUM(emr.order_quantity) = 0 THEN '✓'
                            WHEN ABS(s.quantity - SUM(emr.order_quantity)) <= s.quantity * 0.1 THEN '~'
                            ELSE '✗'
                        END as quantity_match_indicator,
                        
                        s.quantity - SUM(emr.order_quantity) as quantity_variance,
                        
                        -- Overall status
                        CASE 
                            WHEN COUNT(CASE WHEN emr.quantity_check_result = 'FAIL' THEN 1 END) > 0 THEN 'QUANTITY_ISSUES'
                            WHEN COUNT(CASE WHEN emr.delivery_match = 'MISMATCH' THEN 1 END) > 0 THEN 'DELIVERY_ISSUES'
                            ELSE 'GOOD'
                        END as shipment_status
                        
                    FROM stg_fm_orders_shipped_table s
                    INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
                    WHERE s.customer_name LIKE ?
                    GROUP BY s.shipment_id, s.style_code, s.color_description, s.delivery_method, s.quantity
                    """
                    
                    df = pd.read_sql(simplified_query, conn, params=['GREYSON%'])
                    print(f"✅ SIMPLIFIED QUERY SUCCESS: {len(df)} records")
                    
                    if len(df) > 0:
                        print("\nSample simplified enhanced results:")
                        display_cols = ['row_num', 'shipment_id', 'shipment_style', 'style_match_indicator', 
                                       'color_match_indicator', 'delivery_match_indicator', 'match_layers', 
                                       'best_confidence', 'quantity_match_indicator', 'shipment_status']
                        print(df[display_cols].head().to_string(index=False))
                        
                        print(f"\n📊 Simplified enhanced metrics:")
                        print(f"   - Status distribution: {dict(df['shipment_status'].value_counts())}")
                        print(f"   - Layer ranges: {sorted(df['match_layers'].unique())}")
                        print(f"   - Confidence range: {df['best_confidence'].min():.1f} - {df['best_confidence'].max():.1f}")
                        print(f"   - Quantity match indicators: {sorted(df['quantity_match_indicator'].unique())}")
                        
                        # Additional debug for '?' indicators
                        print("\n1c. Investigating '?' indicator records...")
                        try:
                            problem_shipments = df[df['style_match_indicator'] == '?']['shipment_id'].head(3).tolist()
                            if problem_shipments:
                                debug_query = """
                                SELECT 
                                    emr.shipment_id,
                                    emr.style_match,
                                    emr.color_match, 
                                    emr.delivery_match,
                                    emr.quantity_check_result,
                                    emr.match_layer,
                                    emr.match_confidence
                                FROM enhanced_matching_results emr
                                INNER JOIN stg_fm_orders_shipped_table s ON emr.shipment_id = s.shipment_id
                                WHERE s.customer_name LIKE ?
                                  AND emr.shipment_id IN ({})
                                ORDER BY emr.shipment_id
                                """.format(','.join(map(str, problem_shipments)))
                                
                                debug_df = pd.read_sql(debug_query, conn, params=['GREYSON%'])
                                print(f"✅ Debug data: {len(debug_df)} match records for shipments {problem_shipments}")
                                if len(debug_df) > 0:
                                    print("\nActual match data for '?' records:")
                                    print(debug_df.to_string(index=False))
                                    
                                # Now test the MAX logic directly for one shipment
                                test_query = """
                                SELECT 
                                    emr.shipment_id,
                                    MAX(CASE WHEN emr.style_match = 'MATCH' THEN 1 ELSE 0 END) as style_test,
                                    MAX(CASE WHEN emr.color_match = 'MATCH' THEN 1 ELSE 0 END) as color_test,
                                    MAX(CASE WHEN emr.delivery_match = 'MATCH' THEN 1 ELSE 0 END) as delivery_test
                                FROM enhanced_matching_results emr
                                INNER JOIN stg_fm_orders_shipped_table s ON emr.shipment_id = s.shipment_id
                                WHERE s.customer_name LIKE ?
                                  AND emr.shipment_id = ?
                                GROUP BY emr.shipment_id
                                """
                                
                                test_df = pd.read_sql(test_query, conn, params=['GREYSON%', problem_shipments[0]])
                                print(f"\n🔍 MAX logic test for shipment {problem_shipments[0]}:")
                                print(test_df.to_string(index=False))
                            else:
                                print("   No '?' indicators found to debug")
                                
                        except Exception as e3:
                            print(f"❌ DEBUG QUERY FAILED: {e3}")
                        
                except Exception as e2:
                    print(f"❌ SIMPLIFIED QUERY ALSO FAILED: {e2}")
                
            # 2. Test sample data to understand match patterns
            print("\n2. Analyzing match patterns for UI design...")
            try:
                pattern_query = """
                SELECT TOP 10
                    emr.shipment_id,
                    emr.match_layer,
                    emr.match_confidence,
                    emr.style_match,
                    emr.color_match,
                    emr.delivery_match,
                    emr.quantity_check_result,
                    emr.shipment_quantity,
                    emr.order_quantity
                FROM enhanced_matching_results emr
                WHERE emr.customer_name = 'GREYSON' AND emr.po_number = '4755'
                ORDER BY emr.shipment_id, emr.match_confidence DESC
                """
                
                df_patterns = pd.read_sql(pattern_query, conn)
                print(f"✅ Pattern analysis: {len(df_patterns)} match records")
                
                if len(df_patterns) > 0:
                    print("\nMatch pattern samples:")
                    print(df_patterns[['shipment_id', 'match_layer', 'match_confidence', 'style_match', 
                                     'color_match', 'delivery_match', 'quantity_check_result']].head().to_string(index=False))
                    
                    print(f"\nPattern insights:")
                    print(f"   - Match layers: {sorted(df_patterns['match_layer'].unique())}")
                    print(f"   - Style match types: {sorted(df_patterns['style_match'].unique())}")
                    print(f"   - Color match types: {sorted(df_patterns['color_match'].unique())}")
                    print(f"   - Delivery match types: {sorted(df_patterns['delivery_match'].unique())}")
                    print(f"   - Quantity results: {sorted(df_patterns['quantity_check_result'].unique())}")
                
            except Exception as e:
                print(f"❌ PATTERN ANALYSIS FAILED: {e}")
                
            # 3. Generate streamlit implementation code
            print(f"\n3. 🔧 STREAMLIT IMPLEMENTATION RECOMMENDATION:")
            print("=" * 60)
            print("""
ENHANCED SHIPMENT SUMMARY FEATURES:
==================================

1. ROW NUMBERS: ✅ Added with ROW_NUMBER() OVER()
2. MATCH INDICATORS: ✅ Tick/cross/tilde for Style, Color, Delivery, Quantity
3. CONFIDENCE LEVELS: ✅ Best confidence + Average confidence  
4. CONSOLIDATED LAYERS: ✅ STRING_AGG to show [0,1,2] format
5. MATCHED QUANTITIES: ✅ Total matched order quantity vs shipment quantity
6. QUANTITY VARIANCE: ✅ Exact difference and indicator
7. OUTSTANDING REVIEWS: ⚠️  Need to identify review table/status

STREAMLIT COLUMN CONFIGURATION:
=============================

column_config = {
    'row_num': st.column_config.NumberColumn('Row', width='small'),
    'shipment_id': st.column_config.TextColumn('Shipment ID', width='medium'),
    'shipment_style': st.column_config.TextColumn('Style', width='medium'),
    'shipment_color': st.column_config.TextColumn('Color', width='medium'),
    'style_match_indicator': st.column_config.TextColumn('Style ✓', width='small'),
    'color_match_indicator': st.column_config.TextColumn('Color ✓', width='small'),
    'delivery_match_indicator': st.column_config.TextColumn('Delivery ✓', width='small'),
    'quantity_match_indicator': st.column_config.TextColumn('Qty ✓', width='small'),
    'match_layers': st.column_config.TextColumn('Layers', width='small'),
    'best_confidence': st.column_config.ProgressColumn('Confidence', min_value=0, max_value=100),
    'shipment_quantity': st.column_config.NumberColumn('Ship Qty', width='small'),
    'total_matched_order_qty': st.column_config.NumberColumn('Order Qty', width='small'),
    'quantity_variance': st.column_config.NumberColumn('Variance', width='small'),
    'shipment_status': st.column_config.TextColumn('Status', width='medium'),
}

NEXT STEPS:
==========
1. Replace get_shipment_level_summary() with enhanced query
2. Update show_all_shipments_with_status() display logic
3. Add column configuration for better UI presentation
4. Investigate review status integration
            """)

def main():
    parser = argparse.ArgumentParser(description='Debug data flow in order matching system')
    parser.add_argument('--debug01', action='store_true', help='Table and view inventory')
    parser.add_argument('--debug02', action='store_true', help='Delivery method field audit')
    parser.add_argument('--debug03', action='store_true', help='GREYSON PO 4755 data flow analysis')
    parser.add_argument('--debug04', action='store_true', help='Enhanced matcher query testing')
    parser.add_argument('--debug05', action='store_true', help='Models layer usage analysis')
    parser.add_argument('--debug06', action='store_true', help='Cleanup recommendations')
    parser.add_argument('--debug07', action='store_true', help='HITL interface completeness analysis')
    parser.add_argument('--debug08', action='store_true', help='HITL UI improvement recommendations')
    parser.add_argument('--debug09', action='store_true', help='Orders query investigation')
    parser.add_argument('--debug10', action='store_true', help='All Shipments query testing and fixing')
    parser.add_argument('--debug11', action='store_true', help='Layer matching and unmatched records analysis')
    parser.add_argument('--debug12', action='store_true', help='CRITICAL: Quantity variance analysis and unmatched order investigation')
    parser.add_argument('--debug13', action='store_true', help='Test the exact failing shipment summary query from streamlit app')
    parser.add_argument('--debug14', action='store_true', help='Design enhanced shipment summary with row numbers, match indicators, confidence, and consolidated layers')
    parser.add_argument('--all', action='store_true', help='Run all debug queries')
    
    args = parser.parse_args()
    
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    debugger = DataFlowDebugger()
    
    if args.debug01 or args.all:
        debugger.debug01_table_inventory()
    
    if args.debug02 or args.all:
        debugger.debug02_delivery_method_audit()
    
    if args.debug03 or args.all:
        debugger.debug03_greyson_po4755_data_flow()
    
    if args.debug04 or args.all:
        debugger.debug04_enhanced_matcher_queries()
    
    if args.debug05 or args.all:
        debugger.debug05_models_layer_usage()
    
    if args.debug06 or args.all:
        debugger.debug06_cleanup_recommendations()
    
    if args.debug07 or args.all:
        debugger.debug07_hitl_interface_completeness()
    
    if args.debug08 or args.all:
        debugger.debug08_hitl_ui_recommendations()
    
    if args.debug09 or args.all:
        debugger.debug09_orders_investigation()
    
    if args.debug10 or args.all:
        debugger.debug10_test_all_shipments_query()
    
    if args.debug11 or args.all:
        debugger.debug11_layer_matching_analysis()
    
    if args.debug12 or args.all:
        debugger.debug12_quantity_variance_analysis()
    
    if args.debug13 or args.all:
        debugger.debug13_test_failing_shipment_summary_query()
    
    if args.debug14 or args.all:
        debugger.debug14_enhanced_shipment_summary_design()

if __name__ == "__main__":
    main()
