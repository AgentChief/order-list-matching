#!/usr/bin/env python3
"""
Real Data Test - Populate orders from production and run reconciliation
This eliminates dummy data and uses real production order data
"""

import os
import sys
import logging
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_orchestrator import (
    get_connection, 
    populate_orders_from_production,
    run_enhanced_batch_reconciliation,
    execute_sql
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'real_data_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_real_data_matching():
    """Test matching with real production order data"""
    logger.info("=== Real Production Data Test ===")
    logger.info("Step 1: Clear dummy data and populate from real production orders")
    logger.info("Step 2: Run reconciliation with real order data")
    
    try:
        # Get database connection
        logger.info("Connecting to database...")
        connection = get_connection()
        
        # Step 1: Clear existing dummy data and populate from production
        logger.info("\n--- Step 1: Populate Real Order Data ---")
        orders_count = populate_orders_from_production(
            connection, 
            customer="GREYSON", 
            po="4755",
            clear_existing=True
        )
        
        if orders_count == 0:
            logger.warning("No orders found in production data for GREYSON PO 4755")
            logger.info("Let's check what orders are available...")
            
            # Check available orders
            check_sql = """
            SELECT TOP 10
                customer_name,
                po_number,
                style_code,
                color_description,
                quantity,
                order_date
            FROM stg_order_list 
            WHERE customer_name LIKE '%GREYSON%'
            ORDER BY order_date DESC
            """
            
            available_orders = execute_sql(connection, check_sql)
            if available_orders:
                logger.info(f"Found {len(available_orders)} GREYSON orders in production:")
                for i, (customer, po, style, color, qty, date) in enumerate(available_orders[:5]):
                    logger.info(f"  {i+1}. {customer} PO:{po} {style}-{color} Qty:{qty} Date:{date}")
            else:
                logger.warning("No GREYSON orders found in stg_order_list view")
                return
        
        # Step 2: Run reconciliation with real data
        logger.info(f"\n--- Step 2: Run Reconciliation with {orders_count} Real Orders ---")
        
        # Use the original procedure since we fixed the data source
        proc_sql = f"""
        EXEC sp_batch_reconcile_shipments 
            @customer_name = 'GREYSON', 
            @po_number = '4755', 
            @fuzzy_threshold = 85,
            @batch_description = 'Real production data test'
        """
        result = execute_sql(connection, proc_sql)
        
        if result and len(result) > 0:
            batch_id, matched_count = result[0]
            logger.info(f"Reconciliation completed: Batch ID {batch_id}, {matched_count} matches")
        
        # Step 3: Analyze results
        logger.info("\n--- Step 3: Analyze Real Data Results ---")
        
        # Check match types with real data
        analysis_sql = """
        SELECT 
            CASE 
                WHEN s.style_code = o.style_code AND s.color_description = o.color_description THEN 'EXACT_MATCH'
                WHEN s.style_code = o.style_code THEN 'STYLE_MATCH'
                WHEN s.po_number = o.po_number THEN 'PO_MATCH_ONLY'
                ELSE 'NO_MATCH'
            END as match_type,
            COUNT(*) as count,
            AVG(CAST(s.quantity AS FLOAT)) as avg_ship_qty,
            AVG(CAST(o.quantity AS FLOAT)) as avg_order_qty
        FROM stg_fm_orders_shipped_table s
        CROSS JOIN int_orders_extended o
        WHERE s.customer_name = 'GREYSON' AND s.po_number = '4755'
        AND o.customer_name = 'GREYSON' AND o.po_number = '4755'
        GROUP BY 
            CASE 
                WHEN s.style_code = o.style_code AND s.color_description = o.color_description THEN 'EXACT_MATCH'
                WHEN s.style_code = o.style_code THEN 'STYLE_MATCH'
                WHEN s.po_number = o.po_number THEN 'PO_MATCH_ONLY'
                ELSE 'NO_MATCH'
            END
        ORDER BY count DESC
        """
        
        matches = execute_sql(connection, analysis_sql)
        if matches:
            logger.info("Match analysis with real production data:")
            for match_type, count, avg_ship, avg_order in matches:
                logger.info(f"  {match_type}: {count} combinations (avg ship: {avg_ship:.1f}, avg order: {avg_order:.1f})")
        
        # Show specific EXACT matches if any found
        exact_matches_sql = """
        SELECT TOP 5
            s.style_code,
            s.color_description,
            s.quantity as ship_qty,
            o.quantity as order_qty,
            s.delivery_method as ship_method,
            o.delivery_method as order_method
        FROM stg_fm_orders_shipped_table s
        INNER JOIN int_orders_extended o ON 
            s.customer_name = o.customer_name AND 
            s.po_number = o.po_number AND
            s.style_code = o.style_code AND 
            s.color_description = o.color_description
        WHERE s.customer_name = 'GREYSON' AND s.po_number = '4755'
        """
        
        exact_matches = execute_sql(connection, exact_matches_sql)
        if exact_matches:
            logger.info(f"\nFound {len(exact_matches)} EXACT MATCHES with real data:")
            for style, color, ship_qty, order_qty, ship_method, order_method in exact_matches:
                logger.info(f"  ✅ {style}-{color}: Ship {ship_qty} vs Order {order_qty} ({ship_method}/{order_method})")
        else:
            logger.info("No exact matches found - this may be normal if orders and shipments use different style/color codes")
        
    except Exception as e:
        logger.error(f"Real data test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_real_data_matching()
