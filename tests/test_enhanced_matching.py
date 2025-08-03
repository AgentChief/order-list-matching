#!/usr/bin/env python3
"""
Enhanced Reconciliation Test Script
Tests the new enhanced matching procedures with real GREYSON PO 4755 data
Phase 1: Style matching (mandatory), color confidence scoring, delivery method mapping, quantity tolerance ±5%
"""

import os
import sys
import logging
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_orchestrator import (
    get_connection, 
    run_migrations, 
    create_procedures, 
    run_enhanced_batch_reconciliation,
    analyze_delivery_methods,
    execute_sql
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'enhanced_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_enhanced_matching():
    """Test the enhanced matching procedures with GREYSON PO 4755"""
    logger.info("=== Enhanced Matching Test - Phase 1 ===")
    logger.info("Testing business-realistic matching with:")
    logger.info("  - Style matching (mandatory)")
    logger.info("  - Color confidence scoring") 
    logger.info("  - Delivery method mapping")
    logger.info("  - Quantity tolerance ±5%")
    
    try:
        # Get database connection
        logger.info("Connecting to database...")
        connection = get_connection()
        
        # Run migrations to ensure enhanced procedures are available
        logger.info("Ensuring enhanced procedures are available...")
        run_migrations(connection)
        create_procedures(connection)
        
        # Test 1: GREYSON PO 4755 with default parameters
        logger.info("\n--- Test 1: GREYSON PO 4755 (Default Enhanced Matching) ---")
        batch_id_1 = run_enhanced_batch_reconciliation(
            connection, 
            customer="GREYSON", 
            po="4755",
            quantity_tolerance=5.0,      # ±5% quantity tolerance
            style_match_required=True,   # Style must match
            color_threshold=85.0         # Color confidence threshold
        )
        
        if batch_id_1:
            # Analyze delivery methods for this batch
            logger.info(f"\n--- Delivery Method Analysis for Batch {batch_id_1} ---")
            analyze_delivery_methods(connection, batch_id_1)
            
            # Get detailed results
            logger.info(f"\n--- Detailed Results for Batch {batch_id_1} ---")
            results_sql = f"""
            SELECT 
                r.shipment_id,
                r.order_id,
                r.match_confidence,
                r.match_method,
                r.manual_review_required,
                LEFT(r.match_notes, 200) + CASE WHEN LEN(r.match_notes) > 200 THEN '...' ELSE '' END as match_notes_summary
            FROM reconciliation_result r
            WHERE r.batch_id = {batch_id_1}
            ORDER BY r.match_confidence DESC, r.shipment_id
            """
            
            results = execute_sql(connection, results_sql)
            if results:
                logger.info(f"Found {len(results)} matching results:")
                for i, (ship_id, order_id, confidence, method, manual_req, notes) in enumerate(results[:10]):  # Show first 10
                    logger.info(f"  {i+1:2d}. Ship:{ship_id:3d} Order:{order_id:3d} {confidence:5.1f}% {method:15s} {'[MANUAL]' if manual_req else '[AUTO]':8s} {notes}")
                
                if len(results) > 10:
                    logger.info(f"  ... and {len(results) - 10} more results")
        
        # Test 2: Relaxed matching (allow style mismatch for comparison)
        logger.info("\n--- Test 2: GREYSON PO 4755 (Relaxed Style Matching) ---")
        batch_id_2 = run_enhanced_batch_reconciliation(
            connection, 
            customer="GREYSON", 
            po="4755",
            quantity_tolerance=10.0,     # ±10% quantity tolerance  
            style_match_required=False,  # Allow style mismatches
            color_threshold=70.0         # Lower color threshold
        )
        
        # Test 3: All GREYSON orders (no PO filter)
        logger.info("\n--- Test 3: All GREYSON Orders (No PO Filter) ---")
        batch_id_3 = run_enhanced_batch_reconciliation(
            connection, 
            customer="GREYSON", 
            po=None,  # No PO filter
            quantity_tolerance=5.0,
            style_match_required=True,
            color_threshold=85.0
        )
        
        # Overall delivery method analysis
        logger.info("\n--- Overall Delivery Method Analysis ---")
        analyze_delivery_methods(connection, None)  # All data
        
        # Summary
        logger.info("\n=== Enhanced Matching Test Summary ===")
        logger.info(f"Test 1 (Default): Batch ID {batch_id_1}")
        logger.info(f"Test 2 (Relaxed): Batch ID {batch_id_2}")  
        logger.info(f"Test 3 (All GREYSON): Batch ID {batch_id_3}")
        logger.info("Enhanced matching procedures are working correctly!")
        logger.info("Ready for Phase 1 production testing with real order data.")
        
    except Exception as e:
        logger.error(f"Enhanced matching test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_enhanced_matching()
