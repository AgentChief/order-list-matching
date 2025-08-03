"""
Comprehensive Database Query Test Script
Tests all queries used in the Enhanced Streamlit Dashboard
Created: August 1, 2025
Purpose: Validate all database queries before deploying enhanced dashboard
"""

import pandas as pd
import pyodbc
import sys
from pathlib import Path
from datetime import datetime
import traceback

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from auth_helper import get_connection_string

class QueryTester:
    def __init__(self):
        self.connection_string = get_connection_string()
        self.test_results = []
        
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        with self.get_connection() as conn:
            if params:
                return pd.read_sql(query, conn, params=params)
            else:
                return pd.read_sql(query, conn)
    
    def test_query(self, query_name, query, params=None, expected_columns=None):
        """Test a single query and record results"""
        print(f"\n🧪 Testing: {query_name}")
        print("=" * 60)
        
        try:
            result = self.execute_query(query, params)
            
            # Basic checks
            row_count = len(result)
            column_count = len(result.columns) if not result.empty else 0
            
            print(f"✅ Query executed successfully")
            print(f"📊 Result: {row_count} rows, {column_count} columns")
            
            if not result.empty:
                print(f"📋 Columns: {list(result.columns)}")
                print(f"🔍 Sample data:")
                print(result.head(3).to_string(index=False))
                
                # Check expected columns if provided
                if expected_columns:
                    missing_cols = set(expected_columns) - set(result.columns)
                    if missing_cols:
                        print(f"⚠️  Missing expected columns: {missing_cols}")
                    else:
                        print(f"✅ All expected columns present")
            else:
                print("ℹ️  Query returned no data (this may be expected)")
            
            # Record success
            self.test_results.append({
                'query_name': query_name,
                'status': 'SUCCESS',
                'rows': row_count,
                'columns': column_count,
                'error': None
            })
            
            return result
            
        except Exception as e:
            print(f"❌ Query failed: {str(e)}")
            print(f"🔍 Error details: {traceback.format_exc()}")
            
            # Record failure
            self.test_results.append({
                'query_name': query_name,
                'status': 'FAILED',
                'rows': 0,
                'columns': 0,
                'error': str(e)
            })
            
            return None
    
    def run_all_tests(self):
        """Run all query tests"""
        print("🚀 Starting Comprehensive Database Query Tests")
        print("=" * 80)
        
        # Test 1: Customer Summary
        self.test_query(
            "Customer Summary",
            """
            SELECT 
                status,
                COUNT(*) as customer_count
            FROM customers
            GROUP BY status
            ORDER BY customer_count DESC
            """,
            expected_columns=['status', 'customer_count']
        )
        
        # Test 2: Shipment Summary
        self.test_query(
            "Shipment Summary",
            """
            SELECT 
                COUNT(DISTINCT s.shipment_id) as total_shipments,
                COUNT(DISTINCT emr.shipment_id) as matched_shipments,
                COUNT(DISTINCT s.shipment_id) - COUNT(DISTINCT emr.shipment_id) as unmatched_shipments,
                CAST(COUNT(DISTINCT emr.shipment_id) * 100.0 / COUNT(DISTINCT s.shipment_id) AS DECIMAL(5,1)) as match_rate_pct
            FROM stg_fm_orders_shipped_table s
            LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
            """,
            expected_columns=['total_shipments', 'matched_shipments', 'unmatched_shipments', 'match_rate_pct']
        )
        
        # Test 3: Layer Distribution
        self.test_query(
            "Layer Distribution",
            """
            SELECT 
                match_layer,
                COUNT(*) as match_count,
                COUNT(DISTINCT shipment_id) as unique_shipments
            FROM enhanced_matching_results
            GROUP BY match_layer
            ORDER BY 
                CASE 
                    WHEN match_layer = 'LAYER_0' THEN 0
                    WHEN match_layer = 'LAYER_1' THEN 1
                    WHEN match_layer = 'LAYER_2' THEN 2
                    WHEN match_layer = 'LAYER_3' THEN 3
                    ELSE 99
                END
            """,
            expected_columns=['match_layer', 'match_count', 'unique_shipments']
        )
        
        # Test 4: Review Queue Summary
        self.test_query(
            "Review Queue Summary",
            """
            SELECT 
                CASE 
                    WHEN quantity_check_result = 'FAIL' THEN 'Quantity Review'
                    WHEN delivery_match = 'MISMATCH' THEN 'Delivery Review'
                    WHEN match_confidence < 0.8 THEN 'Low Confidence Review'
                    ELSE 'General Review'
                END as review_type,
                COUNT(*) as item_count,
                COUNT(DISTINCT customer_name) as affected_customers
            FROM enhanced_matching_results
            WHERE quantity_check_result = 'FAIL' 
               OR delivery_match = 'MISMATCH'
               OR match_confidence < 0.8
            GROUP BY 
                CASE 
                    WHEN quantity_check_result = 'FAIL' THEN 'Quantity Review'
                    WHEN delivery_match = 'MISMATCH' THEN 'Delivery Review'
                    WHEN match_confidence < 0.8 THEN 'Low Confidence Review'
                    ELSE 'General Review'
                END
            ORDER BY item_count DESC
            """,
            expected_columns=['review_type', 'item_count', 'affected_customers']
        )
        
        # Test 5: System Health Metrics
        self.test_query(
            "System Health Metrics",
            """
            SELECT 
                COUNT(DISTINCT matching_session_id) as total_sessions,
                MAX(created_at) as last_activity,
                COUNT(CASE WHEN created_at > DATEADD(day, -7, GETDATE()) THEN 1 END) as recent_matches,
                COUNT(CASE WHEN quantity_check_result = 'FAIL' THEN 1 END) as quantity_failures,
                COUNT(CASE WHEN delivery_match = 'MISMATCH' THEN 1 END) as delivery_mismatches,
                AVG(match_confidence) as avg_confidence
            FROM enhanced_matching_results
            """,
            expected_columns=['total_sessions', 'last_activity', 'recent_matches', 'quantity_failures', 'delivery_mismatches', 'avg_confidence']
        )
        
        # Test 6: Customer Breakdown
        self.test_query(
            "Customer Breakdown",
            """
            SELECT 
                c.canonical_name,
                c.status,
                COUNT(DISTINCT s.shipment_id) as total_shipments,
                COUNT(DISTINCT emr.shipment_id) as matched_shipments,
                COUNT(DISTINCT CASE WHEN emr.quantity_check_result = 'FAIL' THEN emr.shipment_id END) as qty_issues,
                COUNT(DISTINCT CASE WHEN emr.delivery_match = 'MISMATCH' THEN emr.shipment_id END) as delivery_issues,
                CAST(COUNT(DISTINCT emr.shipment_id) * 100.0 / NULLIF(COUNT(DISTINCT s.shipment_id), 0) AS DECIMAL(5,1)) as match_rate_pct
            FROM customers c
            LEFT JOIN stg_fm_orders_shipped_table s ON s.customer_name LIKE c.canonical_name + '%'
            LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
            GROUP BY c.canonical_name, c.status
            HAVING COUNT(DISTINCT s.shipment_id) > 0
            ORDER BY total_shipments DESC
            """,
            expected_columns=['canonical_name', 'status', 'total_shipments', 'matched_shipments', 'qty_issues', 'delivery_issues', 'match_rate_pct']
        )
        
        # Test 7: Recent Activity
        self.test_query(
            "Recent Activity",
            """
            SELECT 
                CAST(created_at AS DATE) as activity_date,
                COUNT(*) as matches_created,
                COUNT(DISTINCT customer_name) as customers_processed,
                COUNT(DISTINCT matching_session_id) as sessions_run
            FROM enhanced_matching_results
            WHERE created_at > DATEADD(day, -?, GETDATE())
            GROUP BY CAST(created_at AS DATE)
            ORDER BY activity_date DESC
            """,
            params=[7],
            expected_columns=['activity_date', 'matches_created', 'customers_processed', 'sessions_run']
        )
        
        # Test 8: Original Customers Query (for compatibility)
        self.test_query(
            "Original Customers Query",
            """
            SELECT 
                c.id,
                c.canonical_name,
                c.status,
                c.packed_products,
                c.shipped,
                c.master_order_list,
                STRING_AGG(ca.alias_name, ', ') as aliases,
                c.created_at,
                c.updated_at
            FROM customers c
            LEFT JOIN customer_aliases ca ON c.id = ca.customer_id
            GROUP BY c.id, c.canonical_name, c.status, c.packed_products, c.shipped, c.master_order_list, c.created_at, c.updated_at
            ORDER BY c.canonical_name
            """,
            expected_columns=['id', 'canonical_name', 'status', 'packed_products', 'shipped', 'master_order_list', 'aliases', 'created_at', 'updated_at']
        )
        
        # Test 9: Table Existence Checks
        print(f"\n🔍 Testing Table Existence")
        print("=" * 60)
        
        tables_to_check = [
            'customers',
            'customer_aliases', 
            'stg_fm_orders_shipped_table',
            'enhanced_matching_results',
            'stg_order_list'
        ]
        
        for table in tables_to_check:
            try:
                result = self.execute_query(f"SELECT TOP 1 * FROM {table}")
                print(f"✅ Table '{table}' exists ({len(result.columns)} columns)")
            except Exception as e:
                print(f"❌ Table '{table}' missing or inaccessible: {str(e)}")
        
        # Print final summary
        self.print_test_summary()
    
    def print_test_summary(self):
        """Print summary of all test results"""
        print(f"\n📋 TEST SUMMARY")
        print("=" * 80)
        
        total_tests = len(self.test_results)
        successful_tests = len([r for r in self.test_results if r['status'] == 'SUCCESS'])
        failed_tests = total_tests - successful_tests
        
        print(f"🧪 Total Tests: {total_tests}")
        print(f"✅ Successful: {successful_tests}")
        print(f"❌ Failed: {failed_tests}")
        print(f"📊 Success Rate: {successful_tests/total_tests*100:.1f}%")
        
        if failed_tests > 0:
            print(f"\n❌ FAILED TESTS:")
            for result in self.test_results:
                if result['status'] == 'FAILED':
                    print(f"  • {result['query_name']}: {result['error']}")
        
        print(f"\n✅ SUCCESSFUL TESTS:")
        for result in self.test_results:
            if result['status'] == 'SUCCESS':
                print(f"  • {result['query_name']}: {result['rows']} rows, {result['columns']} columns")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        if failed_tests == 0:
            print("  🎉 All queries passed! Enhanced dashboard is ready for deployment.")
        else:
            print("  ⚠️  Fix failed queries before deploying enhanced dashboard.")
            print("  🔧 Check database schema and table structures.")
            print("  📋 Verify table names and column names match expectations.")

def main():
    """Run all database tests"""
    tester = QueryTester()
    
    try:
        tester.run_all_tests()
    except Exception as e:
        print(f"❌ Fatal error during testing: {str(e)}")
        print(f"🔍 Error details: {traceback.format_exc()}")

if __name__ == "__main__":
    main()
