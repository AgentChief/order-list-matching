"""
Database connection and utility functions.
"""
import os
import pyodbc
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Database connection parameters
# In production, these should be loaded from environment variables or a secure config
DB_CONFIG = {
    "driver": "{ODBC Driver 17 for SQL Server}",
    "server": os.getenv("DB_SERVER", "localhost"),
    "database": os.getenv("DB_NAME", "order_matching"),
    "trusted_connection": "yes",  # Windows authentication
}

def get_connection():
    """
    Get a connection to the database.
    """
    try:
        conn_str = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def execute_query(query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
    """
    Execute a query and return the results as a list of dictionaries.
    
    Args:
        query: SQL query to execute
        params: Parameters for the query
        
    Returns:
        List of dictionaries, one per row
    """
    try:
        with get_connection() as conn:
            df = pd.read_sql(query, conn, params=params)
            return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        raise

def execute_non_query(query: str, params: Optional[List] = None) -> int:
    """
    Execute a non-query SQL statement (INSERT, UPDATE, DELETE).
    
    Args:
        query: SQL statement to execute
        params: Parameters for the query
        
    Returns:
        Number of rows affected
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
    except Exception as e:
        logger.error(f"Error executing non-query: {e}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        raise

def execute_stored_procedure(proc_name: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a stored procedure and return the results.
    
    Args:
        proc_name: Name of the stored procedure
        params: Dictionary of parameter names and values
        
    Returns:
        List of dictionaries, one per row
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            if params:
                param_string = ', '.join([f'@{key}=?' for key in params.keys()])
                query = f"EXEC {proc_name} {param_string}"
                cursor.execute(query, list(params.values()))
            else:
                cursor.execute(f"EXEC {proc_name}")
                
            # Get all results
            results = []
            while True:
                try:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    result = []
                    for row in rows:
                        result.append(dict(zip(columns, row)))
                    
                    results.append(result)
                    
                    # Move to next result set
                    if not cursor.nextset():
                        break
                except:
                    # No more results
                    break
            
            return results[0] if results else []
    except Exception as e:
        logger.error(f"Error executing stored procedure: {e}")
        logger.error(f"Procedure: {proc_name}")
        logger.error(f"Params: {params}")
        raise

def get_customer_match_config(customer_name: str) -> Dict[str, Any]:
    """
    Get matching configuration for a specific customer.
    
    Args:
        customer_name: Name of the customer
        
    Returns:
        Dictionary with configuration parameters
    """
    query = """
    SELECT config_type, config_key, config_value
    FROM customer_match_config
    WHERE customer_name = ? AND is_active = 1
    """
    
    try:
        results = execute_query(query, [customer_name])
        
        # Organize results by config_type
        config = {}
        for row in results:
            config_type = row['config_type']
            if config_type not in config:
                config[config_type] = {}
            
            config[config_type][row['config_key']] = row['config_value']
        
        return config
    except Exception as e:
        logger.error(f"Error getting customer match config: {e}")
        # Return default configuration
        return {
            'threshold': {
                'exact_match': '1.0',
                'fuzzy_match': '0.85',
                'uncertain_match': '0.7'
            },
            'attribute_weight': {
                'style': '3.0',
                'color': '2.0',
                'size': '1.0'
            }
        }

def save_reconciliation_result(
    customer_name: str,
    order_id: Optional[int],
    shipment_id: int,
    po_number: str,
    match_status: str,
    confidence_score: Optional[float],
    match_method: str,
    match_details: Optional[Dict] = None,
    is_split_shipment: bool = False,
    split_group_id: Optional[str] = None
) -> int:
    """
    Save a reconciliation result to the database.
    
    Args:
        customer_name: Customer name
        order_id: ID in ORDERS_UNIFIED table (can be None for unmatched)
        shipment_id: ID in FM_orders_shipped table
        po_number: PO number
        match_status: 'matched', 'unmatched', 'uncertain'
        confidence_score: Overall confidence score
        match_method: 'exact', 'fuzzy', 'recordlinkage', 'hitl'
        match_details: JSON with detailed match information
        is_split_shipment: Whether this is part of a split shipment
        split_group_id: Identifier for split shipment group
        
    Returns:
        ID of the created reconciliation result
    """
    query = """
    INSERT INTO reconciliation_result (
        customer_name,
        order_id,
        shipment_id,
        po_number,
        match_status,
        confidence_score,
        match_method,
        match_details,
        is_split_shipment,
        split_group_id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    SELECT SCOPE_IDENTITY() AS id;
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                query,
                [
                    customer_name,
                    order_id,
                    shipment_id,
                    po_number,
                    match_status,
                    confidence_score,
                    match_method,
                    str(match_details) if match_details else None,
                    1 if is_split_shipment else 0,
                    split_group_id
                ]
            )
            row = cursor.fetchone()
            conn.commit()
            return int(row.id)
    except Exception as e:
        logger.error(f"Error saving reconciliation result: {e}")
        raise

def save_attribute_scores(
    reconciliation_id: int,
    attribute_scores: List[Dict[str, Any]]
) -> None:
    """
    Save individual attribute comparison scores.
    
    Args:
        reconciliation_id: ID from reconciliation_result table
        attribute_scores: List of dictionaries with attribute score details
    """
    query = """
    INSERT INTO match_attribute_score (
        reconciliation_id,
        attribute_name,
        order_value,
        shipment_value,
        match_score,
        match_method,
        is_key_attribute,
        weight
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            for score in attribute_scores:
                cursor.execute(
                    query,
                    [
                        reconciliation_id,
                        score['attribute_name'],
                        score['order_value'],
                        score['shipment_value'],
                        score['match_score'],
                        score['match_method'],
                        1 if score.get('is_key_attribute', False) else 0,
                        score.get('weight', 1.0)
                    ]
                )
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving attribute scores: {e}")
        raise

def add_to_hitl_queue(
    reconciliation_id: int,
    priority: int = 5
) -> int:
    """
    Add a reconciliation result to the HITL review queue.
    
    Args:
        reconciliation_id: ID from reconciliation_result table
        priority: Priority (1-10, 10 being highest)
        
    Returns:
        ID in the hitl_queue table
    """
    query = """
    INSERT INTO hitl_queue (
        reconciliation_id,
        priority,
        status
    )
    VALUES (?, ?, 'pending');
    SELECT SCOPE_IDENTITY() AS id;
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [reconciliation_id, priority])
            row = cursor.fetchone()
            conn.commit()
            return int(row.id)
    except Exception as e:
        logger.error(f"Error adding to HITL queue: {e}")
        raise

def get_unmatched_shipments(
    customer_name: Optional[str] = None,
    po_number: Optional[str] = None,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    Get unmatched shipments for reconciliation.
    
    Args:
        customer_name: Filter by customer name (optional)
        po_number: Filter by PO number (optional)
        limit: Maximum number of records to return
        
    Returns:
        List of dictionaries with shipment data
    """
    query = """
    SELECT TOP (?) s.*
    FROM FM_orders_shipped s
    LEFT JOIN reconciliation_result r ON s.id = r.shipment_id
    WHERE r.id IS NULL
    """
    
    params = [limit]
    
    if customer_name:
        query += " AND s.customer_name = ?"
        params.append(customer_name)
    
    if po_number:
        query += " AND s.po_number = ?"
        params.append(po_number)
    
    try:
        return execute_query(query, params)
    except Exception as e:
        logger.error(f"Error getting unmatched shipments: {e}")
        raise

def get_reconciliation_metrics(
    customer_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get reconciliation metrics.
    
    Args:
        customer_name: Filter by customer name (optional)
        start_date: Start date for metrics (YYYY-MM-DD)
        end_date: End date for metrics (YYYY-MM-DD)
        
    Returns:
        Dictionary with metrics
    """
    query = """
    SELECT 
        customer_name,
        reconciliation_date,
        total_records,
        matched_count,
        unmatched_count,
        uncertain_count,
        match_percentage,
        avg_confidence_score,
        methods_used
    FROM matching_metrics_view
    WHERE 1=1
    """
    
    params = []
    
    if customer_name:
        query += " AND customer_name = ?"
        params.append(customer_name)
    
    if start_date:
        query += " AND reconciliation_date >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND reconciliation_date <= ?"
        params.append(end_date)
    
    query += " ORDER BY reconciliation_date DESC"
    
    try:
        return execute_query(query, params)
    except Exception as e:
        logger.error(f"Error getting reconciliation metrics: {e}")
        raise
