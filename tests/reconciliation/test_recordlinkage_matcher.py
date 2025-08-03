"""
Unit tests for recordlinkage matching implementation.
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

from src.reconciliation.recordlinkage_matcher import RecordLinkageMatcher

class TestRecordLinkageMatcher(unittest.TestCase):
    """Test the RecordLinkageMatcher class"""
    
    def setUp(self):
        """Set up test data"""
        # Sample customer
        self.customer_name = "TEST_CUSTOMER"
        
        # Sample configuration
        self.config = {
            'threshold': {
                'exact_match': '1.0',
                'fuzzy_match': '0.85',
                'uncertain_match': '0.7'
            },
            'attribute_weight': {
                'style': '3.0',
                'color': '2.0',
                'size': '1.0'
            },
            'key_attributes': ['style', 'color']
        }
        
        # Sample orders data
        self.orders_data = [
            {
                'id': 1, 
                'customer_name': 'TEST_CUSTOMER', 
                'po_number': '12345', 
                'style': 'ABC123', 
                'color': 'RED', 
                'size': 'M', 
                'quantity': 10
            },
            {
                'id': 2, 
                'customer_name': 'TEST_CUSTOMER', 
                'po_number': '12345', 
                'style': 'DEF456', 
                'color': 'BLUE', 
                'size': 'L', 
                'quantity': 5
            },
            {
                'id': 3, 
                'customer_name': 'TEST_CUSTOMER', 
                'po_number': '12345', 
                'style': 'GHI789', 
                'color': 'GREEN', 
                'size': 'S', 
                'quantity': 15
            }
        ]
        
        # Sample shipments data
        self.shipments_data = [
            {
                'id': 101, 
                'customer_name': 'TEST_CUSTOMER', 
                'po_number': '12345', 
                'style_name': 'ABC123', 
                'color_name': 'RED', 
                'size_name': 'M', 
                'quantity': 10
            },
            {
                'id': 102, 
                'customer_name': 'TEST_CUSTOMER', 
                'po_number': '12345', 
                'style_name': 'DEF456', 
                'color_name': 'NAVY', 
                'size_name': 'L', 
                'quantity': 5
            },
            {
                'id': 103, 
                'customer_name': 'TEST_CUSTOMER', 
                'po_number': '12345', 
                'style_name': 'XYZ999', 
                'color_name': 'BLACK', 
                'size_name': 'XL', 
                'quantity': 8
            }
        ]
        
        # Create DataFrames
        self.orders_df = pd.DataFrame(self.orders_data)
        self.shipments_df = pd.DataFrame(self.shipments_data)
        
        # Rename shipment columns to match expected format
        self.shipments_df = self.shipments_df.rename(columns={
            'style_name': 'style',
            'color_name': 'color',
            'size_name': 'size'
        })
    
    def test_initialization(self):
        """Test initialization of matcher"""
        matcher = RecordLinkageMatcher(self.customer_name, self.config)
        
        self.assertEqual(matcher.customer_name, self.customer_name)
        self.assertEqual(matcher.config, self.config)
        
        # Check thresholds
        self.assertEqual(matcher.thresholds['exact'], 1.0)
        self.assertEqual(matcher.thresholds['fuzzy'], 0.85)
        self.assertEqual(matcher.thresholds['uncertain'], 0.7)
        
        # Check weights
        self.assertEqual(matcher.weights['style'], 3.0)
        self.assertEqual(matcher.weights['color'], 2.0)
        self.assertEqual(matcher.weights['size'], 1.0)
    
    def test_prepare_data(self):
        """Test data preparation"""
        matcher = RecordLinkageMatcher(self.customer_name, self.config)
        
        # Add some problematic data
        test_orders = self.orders_df.copy()
        test_shipments = self.shipments_df.copy()
        
        # Add lowercase and whitespace
        test_orders.loc[0, 'style'] = ' abc123 '
        test_shipments.loc[0, 'color'] = 'red  '
        
        # Add None values
        test_orders.loc[2, 'size'] = None
        
        # Prepare data
        orders, shipments = matcher.prepare_data(test_orders, test_shipments)
        
        # Check cleaning
        self.assertEqual(orders.loc[0, 'style'], 'ABC123')
        self.assertEqual(shipments.loc[0, 'color'], 'RED')
        
        # Check None handling
        self.assertEqual(orders.loc[2, 'size'], '')
    
    def test_matching_exact(self):
        """Test exact matching"""
        matcher = RecordLinkageMatcher(self.customer_name, self.config)
        
        # Perform matching
        matches, uncertain, unmatched = matcher.match(self.orders_df, self.shipments_df)
        
        # Should have one exact match (first row)
        self.assertEqual(len(matches), 1)
        
        # Get the match
        match_idx = matches.index[0]
        self.assertEqual(match_idx[0], 0)  # Order index
        self.assertEqual(match_idx[1], 0)  # Shipment index
        
        # Check match status and method
        self.assertEqual(matches.loc[match_idx, 'match_status'], 'matched')
        self.assertEqual(matches.loc[match_idx, 'match_method'], 'exact')
    
    def test_matching_fuzzy(self):
        """Test fuzzy matching"""
        matcher = RecordLinkageMatcher(self.customer_name, self.config)
        
        # Perform matching
        matches, uncertain, unmatched = matcher.match(self.orders_df, self.shipments_df)
        
        # Second shipment should match with second order (BLUE vs NAVY)
        # This should be a fuzzy match
        
        # Find the match for order index 1 (second order)
        matching_rows = [idx for idx in matches.index if idx[0] == 1]
        
        # Should have one match
        self.assertEqual(len(matching_rows), 1)
        
        # Get the match
        match_idx = matching_rows[0]
        self.assertEqual(match_idx[0], 1)  # Order index
        self.assertEqual(match_idx[1], 1)  # Shipment index
        
        # Check match status and method
        self.assertEqual(matches.loc[match_idx, 'match_status'], 'matched')
        self.assertEqual(matches.loc[match_idx, 'match_method'], 'fuzzy')
    
    def test_unmatched(self):
        """Test unmatched records"""
        matcher = RecordLinkageMatcher(self.customer_name, self.config)
        
        # Perform matching
        matches, uncertain, unmatched = matcher.match(self.orders_df, self.shipments_df)
        
        # Third shipment should be unmatched
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched.iloc[0]['style'], 'XYZ999')
        
        # Third order should also be unmatched (but we don't track unmatched orders directly)
        matching_rows = [idx for idx in matches.index if idx[0] == 2]
        self.assertEqual(len(matching_rows), 0)


if __name__ == '__main__':
    unittest.main()
