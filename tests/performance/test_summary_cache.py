#!/usr/bin/env python3
"""
TASK001 - Test Shipment Summary Cache Performance
Tests the materialized cache performance vs original real-time query
"""

import sys
import time
import pandas as pd
import pyodbc
from pathlib import Path

def get_connection_string():
    """Get connection string"""
    return "Driver={SQL Server};Server=ross-db-srv-test.database.windows.net;Database=ORDERS;UID=admin_ross;PWD=Active@IT2023;Encrypt=yes;TrustServerCertificate=yes;"

def run_performance_test(customer='GREYSON', runs=3):
    """Test cache vs real-time query performance"""
    
    connection_string = get_connection_string()
    
    print("🚀 TASK001 - SHIPMENT SUMMARY CACHE PERFORMANCE TEST")
    print("=" * 60)
    print(f"Testing customer: {customer}")
    print(f"Test runs: {runs}")
    print()
    
    with pyodbc.connect(connection_string) as conn:
        
        # Step 1: Create and populate cache
        print("1. Setting up cache...")
        try:
            # Check if schema exists
            schema_check = conn.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.shipment_summary_cache') AND type in (N'U')
            """).fetchone()[0]
            
            if schema_check == 0:
                print("❌ Cache table not found! Please run db/schema/shipment_summary_cache.sql first")
                return
                
            # Check if stored procedure exists
            proc_check = conn.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.sp_refresh_shipment_summary_cache') AND type in (N'P')
            """).fetchone()[0]
            
            if proc_check == 0:
                print("❌ Stored procedure not found! Please run db/procedures/sp_refresh_shipment_summary_cache.sql first")
                return
                
            # Refresh cache for test customer
            print(f"   Refreshing cache for {customer}...")
            start_refresh = time.time()
            
            cursor = conn.cursor()
            cursor.execute("EXEC sp_refresh_shipment_summary_cache @customer_filter = ?, @debug = 1", customer)
            
            # Get results
            refresh_result = cursor.fetchone()
            refresh_time = time.time() - start_refresh
            
            print(f"   ✅ Cache refreshed in {refresh_time:.2f}s")
            if refresh_result:
                print(f"   Status: {refresh_result[0]}")
                print(f"   Rows processed: {refresh_result[1]}")
                print(f"   Duration: {refresh_result[2]}s")
            print()
            
        except Exception as e:
            print(f"❌ Cache setup failed: {e}")
            return
        
        # Step 2: Test cached query performance
        print("2. Testing CACHED query performance...")
        
        cache_query = """
        SELECT 
            row_number,
            shipment_id,
            style_code,
            color_description,
            delivery_method,
            quantity,
            style_match_indicator,
            color_match_indicator,
            delivery_match_indicator,
            quantity_match_indicator,
            match_count,
            match_layers,
            best_confidence,
            quantity_variance,
            shipment_status
        FROM dbo.shipment_summary_cache
        WHERE customer_name LIKE ?
        ORDER BY row_number
        """
        
        cache_times = []
        cache_df = None
        
        for run in range(runs):
            start_time = time.time()
            cache_df = pd.read_sql(cache_query, conn, params=[f'{customer}%'])
            cache_time = time.time() - start_time
            cache_times.append(cache_time)
            print(f"   Run {run+1}: {cache_time:.3f}s ({len(cache_df)} records)")
        
        avg_cache_time = sum(cache_times) / len(cache_times)
        print(f"   📊 Average cached query time: {avg_cache_time:.3f}s")
        
        # Step 3: Test original real-time query performance
        print("\n3. Testing REAL-TIME query performance...")
        
        # This is the complex query from debug14 that we're replacing
        realtime_query = """
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
            
            -- Match status indicators
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
            
            -- Consolidated layer information
            MIN(emr.match_layer) + '-' + MAX(emr.match_layer) as match_layers,
            
            -- Confidence levels
            MAX(emr.match_confidence) as best_confidence,
            AVG(emr.match_confidence) as avg_confidence,
            
            -- Matched order quantities
            SUM(emr.order_quantity) as total_matched_order_qty,
            
            -- Quantity variance
            CASE 
                WHEN s.quantity - SUM(emr.order_quantity) = 0 THEN 'Y'
                WHEN ABS(s.quantity - SUM(emr.order_quantity)) <= s.quantity * 0.1 THEN 'P'
                ELSE 'N'
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
        
        realtime_times = []
        realtime_df = None
        
        for run in range(runs):
            start_time = time.time()
            realtime_df = pd.read_sql(realtime_query, conn, params=[f'{customer}%'])
            realtime_time = time.time() - start_time
            realtime_times.append(realtime_time)
            print(f"   Run {run+1}: {realtime_time:.3f}s ({len(realtime_df)} records)")
        
        avg_realtime_time = sum(realtime_times) / len(realtime_times)
        print(f"   📊 Average real-time query time: {avg_realtime_time:.3f}s")
        
        # Step 4: Performance comparison
        print("\n4. PERFORMANCE COMPARISON")
        print("-" * 40)
        
        if avg_realtime_time > 0:
            speedup = avg_realtime_time / avg_cache_time
            time_saved = avg_realtime_time - avg_cache_time
            
            print(f"Real-time query:  {avg_realtime_time:.3f}s")
            print(f"Cached query:     {avg_cache_time:.3f}s")
            print(f"Performance gain: {speedup:.1f}x faster")
            print(f"Time saved:       {time_saved:.3f}s per query")
            
            # Success criteria check
            if avg_cache_time < 1.0:
                print("✅ SUCCESS: Target <1 second response time achieved!")
            else:
                print("⚠️  WARNING: Cache query still >1 second")
                
            if speedup > 2.0:
                print("✅ SUCCESS: Significant performance improvement achieved!")
            else:
                print("⚠️  WARNING: Performance improvement less than expected")
        
        # Step 5: Data accuracy validation
        print("\n5. DATA ACCURACY VALIDATION")
        print("-" * 40)
        
        if cache_df is not None and realtime_df is not None:
            print(f"Cached records:    {len(cache_df)}")
            print(f"Real-time records: {len(realtime_df)}")
            
            if len(cache_df) == len(realtime_df):
                print("✅ Record count matches")
                
                # Sample comparison (first 5 records)
                print("\nSample data comparison (first 3 records):")
                print("CACHED DATA:")
                print(cache_df[['shipment_id', 'style_match_indicator', 'color_match_indicator', 
                               'quantity_match_indicator', 'shipment_status']].head(3).to_string(index=False))
                
                print("\nREAL-TIME DATA:")
                print(realtime_df[['shipment_id', 'style_match_indicator', 'color_match_indicator', 
                                 'quantity_match_indicator', 'shipment_status']].head(3).to_string(index=False))
                
            else:
                print("❌ Record count mismatch - investigate data consistency")
        
        # Step 6: Cache statistics
        print("\n6. CACHE STATISTICS")
        print("-" * 40)
        
        try:
            stats_df = pd.read_sql("SELECT * FROM vw_shipment_summary_cache_stats", conn)
            print(f"Total cached records: {stats_df.iloc[0]['total_records']:,}")
            print(f"Unique customers: {stats_df.iloc[0]['unique_customers']}")
            print(f"Good shipments: {stats_df.iloc[0]['good_count']:,}")
            print(f"Quantity issues: {stats_df.iloc[0]['quantity_issues_count']:,}")
            print(f"Delivery issues: {stats_df.iloc[0]['delivery_issues_count']:,}")
            print(f"Unmatched: {stats_df.iloc[0]['unmatched_count']:,}")
            
        except Exception as e:
            print(f"⚠️  Could not retrieve cache statistics: {e}")
        
        print("\n" + "=" * 60)
        print("TASK001 PERFORMANCE TEST COMPLETE")
        
        if avg_cache_time < 1.0 and (avg_realtime_time / avg_cache_time) > 2.0:
            print("🎉 SUCCESS: Performance optimization achieved!")
            print("   Ready for UI integration (next step)")
        else:
            print("⚠️  Review needed: Performance targets not fully met")
            
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test shipment summary cache performance')
    parser.add_argument('--customer', default='GREYSON', help='Customer name to test')
    parser.add_argument('--runs', type=int, default=3, help='Number of test runs')
    
    args = parser.parse_args()
    
    run_performance_test(args.customer, args.runs)
