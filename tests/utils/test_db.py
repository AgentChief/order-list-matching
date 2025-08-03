"""
Integration tests for the database utilities.
"""
import unittest
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from src.utils.db import (
    get_connection,
    execute_query,
    execute_non_query,
    get_customer_match_config,
    save_reconciliation_result,
    save_attribute_scores,
    add_to_hitl_queue
)

class TestDatabaseUtils(unittest.TestCase):
    """Integration tests for database utilities"""
    
    def setUp(self):
        """Set up test data and connection"""
        self.customer_name = "TEST_CUSTOMER"
        self.po_number = "TEST_PO_123"
        
        # Create a temporary reconciliation result for testing
        # This is needed to test related functions
        self.test_reconciliation_id = self._create_test_reconciliation()
    
    def tearDown(self):
        """Clean up test data"""
        # Remove test data from database
        self._cleanup_test_data()
    
    def _create_test_reconciliation(self):
        """Create a test reconciliation record"""
        try:
            # First check if our test record already exists
            query = """
            SELECT id FROM reconciliation_result 
            WHERE customer_name = ? AND po_number = ? AND match_method = 'test'
            """
            results = execute_query(query, [self.customer_name, self.po_number])
            
            if results:
                return results[0]['id']
            
            # Create a new test record
            return save_reconciliation_result(
                customer_name=self.customer_name,
                order_id=None,
                shipment_id=999999,  # Use a high number unlikely to conflict
                po_number=self.po_number,
                match_status='unmatched',
                confidence_score=0.0,
                match_method='test',
                match_details={'test': True}
            )
        except Exception as e:
            print(f"Error creating test reconciliation: {e}")
            return None
    
    def _cleanup_test_data(self):
        """Clean up test data"""
        try:
            # Remove test reconciliation results
            execute_non_query(
                "DELETE FROM reconciliation_result WHERE customer_name = ? AND po_number = ? AND match_method = 'test'",
                [self.customer_name, self.po_number]
            )
            
            # Remove test HITL queue entries
            execute_non_query(
                "DELETE FROM hitl_queue WHERE reconciliation_id = ?",
                [self.test_reconciliation_id]
            )
            
            # Remove test attribute scores
            execute_non_query(
                "DELETE FROM match_attribute_score WHERE reconciliation_id = ?",
                [self.test_reconciliation_id]
            )
        except Exception as e:
            print(f"Error cleaning up test data: {e}")
    
    def test_connection(self):
        """Test database connection"""
        conn = get_connection()
        self.assertIsNotNone(conn)
        conn.close()
    
    def test_execute_query(self):
        """Test execute_query function"""
        # Query to get our test reconciliation
        query = """
        SELECT id, customer_name, po_number, match_method
        FROM reconciliation_result
        WHERE id = ?
        """
        
        results = execute_query(query, [self.test_reconciliation_id])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], self.test_reconciliation_id)
        self.assertEqual(results[0]['customer_name'], self.customer_name)
        self.assertEqual(results[0]['po_number'], self.po_number)
        self.assertEqual(results[0]['match_method'], 'test')
    
    def test_execute_non_query(self):
        """Test execute_non_query function"""
        # Update the test reconciliation
        query = """
        UPDATE reconciliation_result
        SET match_status = 'test_updated'
        WHERE id = ?
        """
        
        rows_affected = execute_non_query(query, [self.test_reconciliation_id])
        
        self.assertEqual(rows_affected, 1)
        
        # Verify the update
        verify_query = """
        SELECT match_status FROM reconciliation_result WHERE id = ?
        """
        
        results = execute_query(verify_query, [self.test_reconciliation_id])
        
        self.assertEqual(results[0]['match_status'], 'test_updated')
    
    def test_get_customer_match_config(self):
        """Test get_customer_match_config function"""
        # Create a test config
        config_query = """
        INSERT INTO customer_match_config
        (customer_name, config_type, config_key, config_value, is_active)
        VALUES
        (?, 'threshold', 'test_threshold', '0.95', 1),
        (?, 'attribute_weight', 'test_attribute', '2.5', 1)
        """
        
        try:
            execute_non_query(config_query, [self.customer_name, self.customer_name])
            
            # Get the config
            config = get_customer_match_config(self.customer_name)
            
            # Verify the config
            self.assertIn('threshold', config)
            self.assertIn('test_threshold', config['threshold'])
            self.assertEqual(config['threshold']['test_threshold'], '0.95')
            
            self.assertIn('attribute_weight', config)
            self.assertIn('test_attribute', config['attribute_weight'])
            self.assertEqual(config['attribute_weight']['test_attribute'], '2.5')
            
        finally:
            # Clean up test config
            execute_non_query(
                "DELETE FROM customer_match_config WHERE customer_name = ?",
                [self.customer_name]
            )
    
    def test_save_attribute_scores(self):
        """Test save_attribute_scores function"""
        # Create test attribute scores
        test_scores = [
            {
                'attribute_name': 'style',
                'order_value': 'TEST_STYLE',
                'shipment_value': 'TEST_STYLE',
                'match_score': 1.0,
                'match_method': 'exact',
                'is_key_attribute': True,
                'weight': 3.0
            },
            {
                'attribute_name': 'color',
                'order_value': 'TEST_COLOR',
                'shipment_value': 'TEST_COLOR_DIFFERENT',
                'match_score': 0.8,
                'match_method': 'fuzzy',
                'is_key_attribute': True,
                'weight': 2.0
            }
        ]
        
        # Save the scores
        save_attribute_scores(self.test_reconciliation_id, test_scores)
        
        # Verify the scores were saved
        query = """
        SELECT attribute_name, order_value, shipment_value, match_score, match_method
        FROM match_attribute_score
        WHERE reconciliation_id = ?
        ORDER BY attribute_name
        """
        
        results = execute_query(query, [self.test_reconciliation_id])
        
        self.assertEqual(len(results), 2)
        
        # Check first score
        self.assertEqual(results[0]['attribute_name'], 'color')
        self.assertEqual(results[0]['order_value'], 'TEST_COLOR')
        self.assertEqual(results[0]['shipment_value'], 'TEST_COLOR_DIFFERENT')
        self.assertEqual(results[0]['match_score'], 0.8)
        self.assertEqual(results[0]['match_method'], 'fuzzy')
        
        # Check second score
        self.assertEqual(results[1]['attribute_name'], 'style')
        self.assertEqual(results[1]['order_value'], 'TEST_STYLE')
        self.assertEqual(results[1]['shipment_value'], 'TEST_STYLE')
        self.assertEqual(results[1]['match_score'], 1.0)
        self.assertEqual(results[1]['match_method'], 'exact')
    
    def test_add_to_hitl_queue(self):
        """Test add_to_hitl_queue function"""
        # Add to HITL queue
        queue_id = add_to_hitl_queue(
            reconciliation_id=self.test_reconciliation_id,
            priority=7
        )
        
        self.assertIsNotNone(queue_id)
        
        # Verify the queue entry
        query = """
        SELECT reconciliation_id, priority, status
        FROM hitl_queue
        WHERE id = ?
        """
        
        results = execute_query(query, [queue_id])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['reconciliation_id'], self.test_reconciliation_id)
        self.assertEqual(results[0]['priority'], 7)
        self.assertEqual(results[0]['status'], 'pending')


if __name__ == '__main__':
    unittest.main()
