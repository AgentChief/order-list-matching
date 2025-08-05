#!/usr/bin/env python3
"""
TASK013 Implementation Testing Suite
Tests the movement table, enhanced matching engine, and unified Streamlit interface
"""

import sys
import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime
import traceback

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Task013TestSuite:
    """Comprehensive test suite for TASK013 implementation"""
    
    def __init__(self):
        self.test_results = []
        self.start_time = datetime.now()
        
    def log_test_result(self, test_name: str, passed: bool, message: str = "", details: str = ""):
        """Log test result"""
        result = {
            'test_name': test_name,
            'passed': passed,
            'message': message,
            'details': details,
            'timestamp': datetime.now()
        }
        self.test_results.append(result)
        
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{status}: {test_name} - {message}")
        
        if details and not passed:
            logger.error(f"Details: {details}")
    
    def test_database_schema(self):
        """Test that database schema is properly created"""
        logger.info("Testing database schema...")
        
        try:
            from auth_helper import get_connection_string
            import pyodbc
            
            conn = pyodbc.connect(get_connection_string())
            cursor = conn.cursor()
            
            # Test fact_order_movements table exists
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'fact_order_movements'
            """)
            
            if cursor.fetchone()[0] == 1:
                self.log_test_result(
                    "Database Schema - fact_order_movements", 
                    True, 
                    "Movement table exists"
                )
            else:
                self.log_test_result(
                    "Database Schema - fact_order_movements", 
                    False, 
                    "Movement table not found"
                )
            
            # Test movement views exist
            views_to_check = [
                'vw_order_status_summary',
                'vw_open_order_book', 
                'vw_movement_analytics'
            ]
            
            for view_name in views_to_check:
                cursor.execute("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS 
                    WHERE TABLE_NAME = ?
                """, view_name)
                
                exists = cursor.fetchone()[0] == 1
                self.log_test_result(
                    f"Database Schema - {view_name}",
                    exists,
                    "View exists" if exists else "View not found"
                )
            
            # Test stored procedures exist
            procedures_to_check = [
                'sp_capture_order_placed',
                'sp_capture_shipment_created',
                'sp_capture_shipment_shipped',
                'sp_capture_reconciliation_event'
            ]
            
            for proc_name in procedures_to_check:
                cursor.execute("""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES 
                    WHERE ROUTINE_NAME = ? AND ROUTINE_TYPE = 'PROCEDURE'
                """, proc_name)
                
                exists = cursor.fetchone()[0] == 1
                self.log_test_result(
                    f"Database Schema - {proc_name}",
                    exists,
                    "Procedure exists" if exists else "Procedure not found"
                )
            
            conn.close()
            
        except Exception as e:
            self.log_test_result(
                "Database Schema", 
                False, 
                "Database connection or schema test failed",
                str(e)
            )
    
    def test_enhanced_matching_engine(self):
        """Test the enhanced matching engine"""
        logger.info("Testing enhanced matching engine...")
        
        try:
            # Import the enhanced matching engine
            sys.path.append(str(project_root / 'src' / 'reconciliation'))
            from enhanced_matching_engine import EnhancedMatchingEngine
            
            # Test engine instantiation
            engine = EnhancedMatchingEngine()
            self.log_test_result(
                "Enhanced Matching Engine - Instantiation",
                True,
                "Engine created successfully"
            )
            
            # Test database connection
            try:
                conn = engine.get_connection()
                conn.close()
                self.log_test_result(
                    "Enhanced Matching Engine - Database Connection",
                    True,
                    "Database connection successful"
                )
            except Exception as e:
                self.log_test_result(
                    "Enhanced Matching Engine - Database Connection",
                    False,
                    "Database connection failed",
                    str(e)
                )
            
            # Test session management
            try:
                batch_id = engine.start_matching_session("TEST_CUSTOMER", "TEST_PO", "Test session")
                if batch_id and batch_id > 0:
                    self.log_test_result(
                        "Enhanced Matching Engine - Session Management",
                        True,
                        f"Session started with batch_id: {batch_id}"
                    )
                    
                    # End the test session
                    engine.end_matching_session("COMPLETED", 0, 0)
                else:
                    self.log_test_result(
                        "Enhanced Matching Engine - Session Management",
                        False,
                        "Failed to start session"
                    )
            except Exception as e:
                self.log_test_result(
                    "Enhanced Matching Engine - Session Management",
                    False,
                    "Session management failed",
                    str(e)
                )
            
        except Exception as e:
            self.log_test_result(
                "Enhanced Matching Engine",
                False,
                "Engine import or basic functionality failed",
                str(e)
            )
    
    def test_movement_table_operations(self):
        """Test movement table operations"""
        logger.info("Testing movement table operations...")
        
        try:
            from auth_helper import get_connection_string
            import pyodbc
            
            conn = pyodbc.connect(get_connection_string())
            cursor = conn.cursor()
            
            # Test inserting a sample movement
            test_order_id = f"TEST_ORDER_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            cursor.execute("""
                EXEC sp_capture_order_placed 
                @order_id = ?, @customer_name = ?, @po_number = ?,
                @style_code = ?, @color_description = ?, @order_quantity = ?,
                @order_type = ?
            """, test_order_id, 'TEST_CUSTOMER', 'TEST_PO', 'TEST_STYLE', 'TEST_COLOR', 10, 'STANDARD')
            
            result = cursor.fetchone()
            if result and result[0]:
                movement_id = result[0]
                self.log_test_result(
                    "Movement Table - Order Insertion",
                    True,
                    f"Order movement created with ID: {movement_id}"
                )
                
                # Test querying the movement
                cursor.execute("""
                    SELECT COUNT(*) FROM fact_order_movements 
                    WHERE order_id = ? AND movement_type = 'ORDER_PLACED'
                """, test_order_id)
                
                count = cursor.fetchone()[0]
                if count > 0:
                    self.log_test_result(
                        "Movement Table - Order Query",
                        True,
                        "Order movement successfully queried"
                    )
                else:
                    self.log_test_result(
                        "Movement Table - Order Query",
                        False,
                        "Order movement not found in query"
                    )
                
                # Cleanup test data
                cursor.execute("DELETE FROM fact_order_movements WHERE order_id = ?", test_order_id)
                conn.commit()
                
            else:
                self.log_test_result(
                    "Movement Table - Order Insertion",
                    False,
                    "Failed to insert order movement"
                )
            
            conn.close()
            
        except Exception as e:
            self.log_test_result(
                "Movement Table Operations",
                False,
                "Movement table operations failed",
                str(e)
            )
    
    def test_unified_streamlit_interface(self):
        """Test unified Streamlit interface components"""
        logger.info("Testing unified Streamlit interface...")
        
        try:
            # Import the unified interface
            sys.path.append(str(project_root / 'src' / 'ui'))
            from unified_streamlit_app import UnifiedDataManager
            
            # Test data manager instantiation
            data_mgr = UnifiedDataManager()
            self.log_test_result(
                "Unified Interface - Data Manager",
                True,
                "Data manager created successfully"
            )
            
            # Test database connection
            try:
                conn = data_mgr.get_connection()
                conn.close()
                self.log_test_result(
                    "Unified Interface - Database Connection",
                    True,
                    "Database connection successful"
                )
            except Exception as e:
                self.log_test_result(
                    "Unified Interface - Database Connection",
                    False,
                    "Database connection failed",
                    str(e)
                )
            
            # Test system overview query
            try:
                overview = data_mgr.get_system_overview()
                if isinstance(overview, dict) and 'total_movements' in overview:
                    self.log_test_result(
                        "Unified Interface - System Overview",
                        True,
                        f"System overview loaded with {overview.get('total_movements', 0)} movements"
                    )
                else:
                    self.log_test_result(
                        "Unified Interface - System Overview",
                        False,
                        "System overview query returned unexpected format"
                    )
            except Exception as e:
                self.log_test_result(
                    "Unified Interface - System Overview",
                    False,
                    "System overview query failed",
                    str(e)
                )
            
        except Exception as e:
            self.log_test_result(
                "Unified Streamlit Interface",
                False,
                "Interface import or basic functionality failed",
                str(e)
            )
    
    def test_integration_workflow(self):
        """Test end-to-end integration workflow"""
        logger.info("Testing integration workflow...")
        
        try:
            # This would test the complete workflow:
            # 1. Create sample order and shipment data
            # 2. Run enhanced matching
            # 3. Verify movement table is updated
            # 4. Check interface can display results
            
            # For now, we'll do a basic integration test
            from auth_helper import get_connection_string
            import pyodbc
            
            conn = pyodbc.connect(get_connection_string())
            cursor = conn.cursor()
            
            # Check if we have some basic data for integration testing
            cursor.execute("SELECT COUNT(*) FROM FACT_ORDER_LIST")
            order_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM FM_orders_shipped")
            shipment_count = cursor.fetchone()[0]
            
            if order_count > 0 and shipment_count > 0:
                self.log_test_result(
                    "Integration Workflow - Data Availability",
                    True,
                    f"Found {order_count} orders and {shipment_count} shipments for testing"
                )
            else:
                self.log_test_result(
                    "Integration Workflow - Data Availability",
                    False,
                    "Insufficient data for integration testing"
                )
            
            conn.close()
            
        except Exception as e:
            self.log_test_result(
                "Integration Workflow",
                False,
                "Integration workflow test failed",
                str(e)
            )
    
    def test_performance_expectations(self):
        """Test that performance expectations are met"""
        logger.info("Testing performance expectations...")
        
        try:
            from auth_helper import get_connection_string
            import pyodbc
            import time
            
            conn = pyodbc.connect(get_connection_string())
            cursor = conn.cursor()
            
            # Test cache query performance (should be <1s)
            start_time = time.time()
            cursor.execute("SELECT COUNT(*) FROM shipment_summary_cache")
            cache_count = cursor.fetchone()[0]
            cache_query_time = time.time() - start_time
            
            if cache_query_time < 1.0:
                self.log_test_result(
                    "Performance - Cache Query",
                    True,
                    f"Cache query took {cache_query_time:.3f}s (target: <1s)"
                )
            else:
                self.log_test_result(
                    "Performance - Cache Query",
                    False,
                    f"Cache query took {cache_query_time:.3f}s (target: <1s)"
                )
            
            # Test movement analytics view performance
            start_time = time.time()
            cursor.execute("SELECT * FROM vw_movement_analytics")
            analytics = cursor.fetchone()
            analytics_query_time = time.time() - start_time
            
            if analytics_query_time < 2.0:
                self.log_test_result(
                    "Performance - Analytics View",
                    True,
                    f"Analytics query took {analytics_query_time:.3f}s (target: <2s)"
                )
            else:
                self.log_test_result(
                    "Performance - Analytics View",
                    False,
                    f"Analytics query took {analytics_query_time:.3f}s (target: <2s)"
                )
            
            conn.close()
            
        except Exception as e:
            self.log_test_result(
                "Performance Expectations",
                False,
                "Performance testing failed",
                str(e)
            )
    
    def run_all_tests(self):
        """Run all tests in the suite"""
        logger.info("Starting TASK013 Implementation Test Suite...")
        logger.info("=" * 60)
        
        # Run all test methods
        test_methods = [
            self.test_database_schema,
            self.test_enhanced_matching_engine,
            self.test_movement_table_operations,
            self.test_unified_streamlit_interface,
            self.test_integration_workflow,
            self.test_performance_expectations
        ]
        
        for test_method in test_methods:
            try:
                test_method()
            except Exception as e:
                logger.error(f"Test method {test_method.__name__} failed with exception: {str(e)}")
                logger.error(traceback.format_exc())
        
        # Generate summary report
        self.generate_summary_report()
    
    def generate_summary_report(self):
        """Generate and display summary report"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        total_tests = len(self.test_results)
        passed_tests = len([r for r in self.test_results if r['passed']])
        failed_tests = total_tests - passed_tests
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        logger.info("=" * 60)
        logger.info("TASK013 IMPLEMENTATION TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Tests: {total_tests}")
        logger.info(f"Passed: {passed_tests}")
        logger.info(f"Failed: {failed_tests}")
        logger.info(f"Success Rate: {success_rate:.1f}%")
        logger.info(f"Duration: {duration.total_seconds():.2f} seconds")
        logger.info("=" * 60)
        
        if failed_tests > 0:
            logger.info("FAILED TESTS:")
            for result in self.test_results:
                if not result['passed']:
                    logger.info(f"❌ {result['test_name']}: {result['message']}")
                    if result['details']:
                        logger.info(f"   Details: {result['details']}")
        
        logger.info("=" * 60)
        
        # Create results DataFrame for potential export
        results_df = pd.DataFrame(self.test_results)
        results_file = f"task013_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        results_df.to_csv(results_file, index=False)
        logger.info(f"Detailed results saved to: {results_file}")
        
        return success_rate >= 80  # Return True if 80% or more tests pass

def main():
    """Main execution"""
    test_suite = Task013TestSuite()
    success = test_suite.run_all_tests()
    
    if success:
        logger.info("🎉 TASK013 implementation tests completed successfully!")
        return 0
    else:
        logger.error("❌ TASK013 implementation tests failed!")
        return 1

if __name__ == "__main__":
    exit(main())