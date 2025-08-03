"""
Main reconciliation script using the recordlinkage approach.
"""
import argparse
import pandas as pd
import numpy as np
import logging
from pathlib import Path
import sys
import os
from datetime import datetime

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.utils.db import (
    execute_query,
    execute_non_query,
    save_reconciliation_result,
    get_reconciliation_metrics,
    get_unmatched_shipments,
    get_connection
)
from src.reconciliation.recordlinkage_matcher import reconcile_with_recordlinkage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(project_root / "logs" / f"reconciliation_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_data_from_db(customer_name: str, po_number: str = None) -> tuple:
    """
    Load orders and shipments data from the database.
    
    Args:
        customer_name: Customer name
        po_number: Optional PO number filter
        
    Returns:
        Tuple of (orders_df, shipments_df)
    """
    logger.info(f"Loading data for {customer_name}" + (f", PO: {po_number}" if po_number else ""))
    
    # Query to get orders
    orders_query = """
    SELECT * FROM ORDERS_UNIFIED
    WHERE customer_name = ?
    """
    
    # Query to get shipments
    shipments_query = """
    SELECT * FROM FM_orders_shipped
    WHERE customer_name = ?
    """
    
    params = [customer_name]
    
    # Add PO filter if provided
    if po_number:
        orders_query += " AND po_number = ?"
        shipments_query += " AND po_number = ?"
        params.append(po_number)
    
    try:
        # Load data into DataFrames
        orders_data = execute_query(orders_query, params)
        shipments_data = execute_query(shipments_query, params)
        
        # Convert to pandas DataFrames
        orders_df = pd.DataFrame(orders_data)
        shipments_df = pd.DataFrame(shipments_data)
        
        logger.info(f"Loaded {len(orders_df)} orders and {len(shipments_df)} shipments")
        
        return orders_df, shipments_df
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise


def save_job_record(customer_name: str, po_number: str = None, job_parameters: dict = None) -> int:
    """
    Create a job record for tracking reconciliation progress.
    
    Args:
        customer_name: Customer name
        po_number: Optional PO number
        job_parameters: Optional parameters for the job
        
    Returns:
        Job ID
    """
    query = """
    INSERT INTO reconciliation_job (
        job_name,
        customer_name,
        po_number,
        job_parameters,
        status,
        start_time,
        created_at
    )
    VALUES (
        ?,
        ?,
        ?,
        ?,
        'running',
        GETDATE(),
        GETDATE()
    );
    SELECT SCOPE_IDENTITY() AS id;
    """
    
    job_name = f"Reconciliation - {customer_name}" + (f" - PO {po_number}" if po_number else "")
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                query,
                [
                    job_name,
                    customer_name,
                    po_number,
                    str(job_parameters) if job_parameters else None
                ]
            )
            row = cursor.fetchone()
            conn.commit()
            
            job_id = int(row.id)
            logger.info(f"Created job record with ID {job_id}")
            
            return job_id
    except Exception as e:
        logger.error(f"Error creating job record: {e}")
        return None


def update_job_record(job_id: int, status: str, results: dict = None) -> bool:
    """
    Update a job record with results.
    
    Args:
        job_id: Job ID
        status: Job status ('completed', 'failed')
        results: Results to save
        
    Returns:
        True if successful, False otherwise
    """
    query = """
    UPDATE reconciliation_job
    SET 
        status = ?,
        end_time = GETDATE(),
        total_records = ?,
        matched_count = ?,
        unmatched_count = ?,
        uncertain_count = ?
    WHERE 
        id = ?
    """
    
    try:
        total = results.get('total_shipments', 0) if results else 0
        matched = results.get('matched_count', 0) if results else 0
        unmatched = results.get('unmatched_count', 0) if results else 0
        uncertain = results.get('uncertain_count', 0) if results else 0
        
        execute_non_query(query, [status, total, matched, unmatched, uncertain, job_id])
        logger.info(f"Updated job {job_id} with status {status}")
        
        return True
    except Exception as e:
        logger.error(f"Error updating job record: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Reconcile orders and shipments using recordlinkage")
    parser.add_argument("--customer", required=True, help="Customer name")
    parser.add_argument("--po", help="PO number (optional)")
    parser.add_argument("--fuzzy-threshold", type=float, default=0.85, help="Threshold for fuzzy matching (0.0-1.0)")
    args = parser.parse_args()
    
    try:
        # Create job record
        job_parameters = {
            'customer': args.customer,
            'po': args.po,
            'fuzzy_threshold': args.fuzzy_threshold
        }
        job_id = save_job_record(args.customer, args.po, job_parameters)
        
        # Load data
        orders_df, shipments_df = load_data_from_db(args.customer, args.po)
        
        if len(orders_df) == 0 or len(shipments_df) == 0:
            logger.warning("No data to reconcile")
            if job_id:
                update_job_record(job_id, 'completed', {
                    'total_shipments': 0,
                    'matched_count': 0,
                    'unmatched_count': 0,
                    'uncertain_count': 0
                })
            return
        
        # Set up configuration
        config = {
            'threshold': {
                'exact_match': '1.0',
                'fuzzy_match': str(args.fuzzy_threshold),
                'uncertain_match': '0.7'
            },
            'attribute_weight': {
                'style': '3.0',
                'color': '2.0',
                'size': '1.0'
            },
            'key_attributes': ['style', 'color']
        }
        
        # Run reconciliation
        results = reconcile_with_recordlinkage(
            args.customer,
            args.po or 'ALL',
            orders_df,
            shipments_df,
            config
        )
        
        # Update job record
        if job_id:
            update_job_record(job_id, 'completed', results)
        
        # Print summary
        print("\nReconciliation Summary:")
        print(f"Customer: {args.customer}")
        if args.po:
            print(f"PO: {args.po}")
        print(f"Total Orders: {results['total_orders']}")
        print(f"Total Shipments: {results['total_shipments']}")
        print(f"Matched: {results['matched_count']} ({results['match_percentage']}%)")
        print(f"Uncertain: {results['uncertain_count']}")
        print(f"Unmatched: {results['unmatched_count']}")
        
    except Exception as e:
        logger.error(f"Error in reconciliation: {e}", exc_info=True)
        if job_id:
            update_job_record(job_id, 'failed')
        raise


if __name__ == "__main__":
    main()
