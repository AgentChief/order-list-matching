#!/usr/bin/env python3
"""
TASK001 - Simple Cache Performance Test
Tests cache functionality and basic performance
"""

import time
import pyodbc

def get_connection_string():
    """Get connection string"""
    return "Driver={SQL Server};Server=ross-db-srv-test.database.windows.net;Database=ORDERS;UID=admin_ross;PWD=Active@IT2023;Encrypt=yes;TrustServerCertificate=yes;"

def test_cache_functionality():
    """Test basic cache functionality"""
    
    print("🚀 TASK001 - CACHE FUNCTIONALITY TEST")
    print("=" * 50)
    
    try:
        with pyodbc.connect(get_connection_string()) as conn:
            cursor = conn.cursor()
            
            print("1. Testing cache schema...")
            
            # Check table exists
            table_check = cursor.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.shipment_summary_cache') AND type in (N'U')
            """).fetchone()[0]
            
            if table_check > 0:
                print("   ✅ Cache table exists")
            else:
                print("   ❌ Cache table missing")
                return False
            
            # Check stored procedure exists
            proc_check = cursor.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.sp_refresh_shipment_summary_cache') AND type in (N'P')
            """).fetchone()[0]
            
            if proc_check > 0:
                print("   ✅ Stored procedure exists")
            else:
                print("   ❌ Stored procedure missing")
                return False
            
            print("\n2. Testing cache refresh...")
            
            # Test full refresh
            start_time = time.time()
            result = cursor.execute("EXEC dbo.sp_refresh_shipment_summary_cache @refresh_type='FULL', @debug=1").fetchone()
            refresh_duration = time.time() - start_time
            
            if result and result[0] == 'SUCCESS':
                print(f"   ✅ Cache refresh successful in {refresh_duration:.3f}s")
                print(f"   Rows processed: {result[1]}")
            else:
                print(f"   ❌ Cache refresh failed: {result}")
                return False
            
            print("\n3. Testing cache queries...")
            
            # Test simple cache query
            start_time = time.time()
            cache_results = cursor.execute("""
                SELECT COUNT(*) as total_records,
                       COUNT(DISTINCT customer_name) as unique_customers,
                       AVG(CAST(best_confidence AS FLOAT)) as avg_confidence
                FROM dbo.shipment_summary_cache
            """).fetchone()
            query_duration = time.time() - start_time
            
            if cache_results:
                print(f"   ✅ Cache query successful in {query_duration:.3f}s")
                print(f"   Total records: {cache_results[0]}")
                print(f"   Unique customers: {cache_results[1]}")
                print(f"   Average confidence: {cache_results[2]:.2f}" if cache_results[2] else "   Average confidence: N/A")
            else:
                print("   ❌ Cache query returned no results")
                return False
            
            print("\n4. Testing performance view...")
            
            # Test statistics view
            start_time = time.time()
            stats_results = cursor.execute("SELECT * FROM dbo.vw_shipment_summary_cache_stats").fetchone()
            stats_duration = time.time() - start_time
            
            if stats_results:
                print(f"   ✅ Statistics view query successful in {stats_duration:.3f}s")
                print(f"   Total records: {stats_results[0]}")
                print(f"   Unique customers: {stats_results[1]}")
                print(f"   GOOD status count: {stats_results[3]}")
            else:
                print("   ❌ Statistics view returned no results")
                return False
            
            print("\n✅ ALL CACHE TESTS PASSED!")
            print(f"Cache is ready for integration into Streamlit UI")
            
            # Performance summary
            print("\n📊 PERFORMANCE SUMMARY")
            print(f"Cache refresh: {refresh_duration:.3f}s")
            print(f"Cache query: {query_duration:.3f}s") 
            print(f"Stats view: {stats_duration:.3f}s")
            
            # Check if performance meets <1 second target
            max_time = max(query_duration, stats_duration)
            if max_time < 1.0:
                print(f"✅ Performance target MET: {max_time:.3f}s < 1.0s")
            else:
                print(f"⚠️ Performance target MISSED: {max_time:.3f}s >= 1.0s")
            
            return True
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_cache_functionality()
    exit(0 if success else 1)
