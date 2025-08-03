#!/usr/bin/env python3
"""
SQL Query Helper for Order Reconciliation Analysis
Provides easy functions to investigate reconciliation results
"""

import argparse
import logging
from pathlib import Path
import pyodbc
import pandas as pd
import yaml
from tabulate import tabulate

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(path="config/config.yaml"):
    """Load configuration from YAML file"""
    with open(path, "r") as f:
        config = yaml.safe_load(f)
        return config

def get_connection_string(db_key="orders"):
    """Get connection string from config file"""
    config = load_config()
    db_config = config.get('databases', {}).get(db_key, {})
    
    if 'conn_str' in db_config:
        return db_config['conn_str']
    
    # Fall back to parameters if conn_str not available
    return "Driver={SQL Server};Server=localhost;Database=order_matching;Trusted_Connection=yes;"

def execute_query(connection, query):
    """Execute query and return pandas DataFrame"""
    try:
        df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        logger.error(f"Query failed: {e}")
        logger.error(f"Query: {query}")
        return None

def print_results(df, title):
    """Print DataFrame in a nice table format"""
    if df is None or df.empty:
        print(f"\n=== {title} ===")
        print("No data found.")
        return
    
    print(f"\n=== {title} ===")
    print(f"Rows: {len(df)}")
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))

def analyze_staging_data(connection, customer=None, po=None):
    """Analyze what's in the staging table"""
    where_clause = "WHERE 1=1"
    if customer:
        where_clause += f" AND customer_name = '{customer}'"
    if po:
        where_clause += f" AND po_number = '{po}'"
    
    query = f"""
    SELECT TOP 10
        shipment_id,
        customer_name, 
        po_number,
        style_code,
        color_description,
        style_color_key,
        customer_po_key,
        quantity,
        reconciliation_status,
        matched_order_id
    FROM stg_fm_orders_shipped_table
    {where_clause}
    ORDER BY shipment_id
    """
    
    df = execute_query(connection, query)
    print_results(df, f"Staging Data {f'for {customer}' if customer else ''}{f' PO {po}' if po else ''}")
    
    # Summary stats
    summary_query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT customer_name) as unique_customers,
        COUNT(DISTINCT po_number) as unique_pos,
        COUNT(DISTINCT style_color_key) as unique_style_colors,
        SUM(quantity) as total_quantity
    FROM stg_fm_orders_shipped_table
    {where_clause}
    """
    
    summary_df = execute_query(connection, summary_query)
    print_results(summary_df, "Staging Summary")

def analyze_orders_data(connection, customer=None, po=None):
    """Analyze what's in the orders extended table"""
    where_clause = "WHERE 1=1"
    if customer:
        where_clause += f" AND customer_name = '{customer}'"
    if po:
        where_clause += f" AND po_number = '{po}'"
    
    query = f"""
    SELECT TOP 10
        order_id,
        customer_name,
        po_number, 
        style_code,
        color_description,
        style_color_key,
        quantity
    FROM int_orders_extended
    {where_clause}
    ORDER BY id
    """
    
    df = execute_query(connection, query)
    print_results(df, f"Orders Data {f'for {customer}' if customer else ''}{f' PO {po}' if po else ''}")
    
    # Summary stats
    summary_query = f"""
    SELECT 
        COUNT(*) as total_orders,
        COUNT(DISTINCT customer_name) as unique_customers,
        COUNT(DISTINCT po_number) as unique_pos,
        COUNT(DISTINCT style_color_key) as unique_style_colors,
        SUM(quantity) as total_quantity
    FROM int_orders_extended
    {where_clause}
    """
    
    summary_df = execute_query(connection, summary_query)
    print_results(summary_df, "Orders Summary")

def analyze_reconciliation_results(connection, batch_id=None):
    """Analyze reconciliation batch results"""
    where_clause = "WHERE 1=1"
    if batch_id:
        where_clause += f" AND batch_id = {batch_id}"
    
    # Batch summary
    batch_query = f"""
    SELECT 
        id as batch_id,
        name,
        start_time,
        end_time,
        status,
        matched_count,
        unmatched_count,
        fuzzy_threshold
    FROM reconciliation_batch
    ORDER BY id DESC
    """
    
    batch_df = execute_query(connection, batch_query)
    print_results(batch_df, "Reconciliation Batches")
    
    # Results details
    if batch_id:
        results_query = f"""
        SELECT TOP 20
            r.batch_id,
            r.shipment_id,
            r.order_id,
            r.match_status,
            r.match_confidence,
            r.match_method,
            s.customer_name,
            s.po_number,
            s.style_code,
            s.color_description
        FROM reconciliation_result r
        LEFT JOIN stg_fm_orders_shipped_table s ON r.shipment_id = s.shipment_id
        WHERE r.batch_id = {batch_id}
        ORDER BY r.match_confidence DESC
        """
        
        results_df = execute_query(connection, results_query)
        print_results(results_df, f"Reconciliation Results for Batch {batch_id}")

def analyze_key_matching(connection, customer=None, po=None):
    """Analyze the key matching logic"""
    where_clause = "WHERE 1=1"
    if customer:
        where_clause += f" AND s.customer_name = '{customer}'"
    if po:
        where_clause += f" AND s.po_number = '{po}'"
    
    # Check for potential matches by key
    query = f"""
    SELECT 
        s.customer_name,
        s.po_number,
        s.style_color_key as shipment_key,
        s.customer_po_key,
        o.style_color_key as order_key,
        CONCAT(o.customer_name, '-', o.po_number) as order_customer_po_key,
        CASE 
            WHEN s.style_color_key = o.style_color_key 
             AND s.customer_po_key = CONCAT(o.customer_name, '-', o.po_number)
            THEN 'EXACT_MATCH'
            WHEN s.style_color_key = o.style_color_key THEN 'STYLE_MATCH_ONLY'
            WHEN s.customer_po_key = CONCAT(o.customer_name, '-', o.po_number) THEN 'PO_MATCH_ONLY'
            ELSE 'NO_MATCH'
        END as match_type,
        s.quantity as shipment_qty,
        o.quantity as order_qty
    FROM stg_fm_orders_shipped_table s
    CROSS JOIN int_orders_extended o
    {where_clause}
    """
    
    df = execute_query(connection, query)
    if df is not None and not df.empty:
        # Filter to show interesting cases
        matches = df[df['match_type'] != 'NO_MATCH']
        print_results(matches, "Potential Key Matches")
        
        # Summary by match type
        summary = df['match_type'].value_counts().reset_index()
        summary.columns = ['match_type', 'count']
        print_results(summary, "Match Type Summary")

def create_analysis_report(connection, customer=None, po=None, batch_id=None):
    """Create a comprehensive analysis report"""
    print("=" * 80)
    print(f"RECONCILIATION ANALYSIS REPORT")
    print(f"Generated: {pd.Timestamp.now()}")
    if customer or po:
        print(f"Filter: {customer or 'ALL'} PO {po or 'ALL'}")
    print("=" * 80)
    
    # 1. Staging data analysis
    analyze_staging_data(connection, customer, po)
    
    # 2. Orders data analysis  
    analyze_orders_data(connection, customer, po)
    
    # 3. Key matching analysis
    analyze_key_matching(connection, customer, po)
    
    # 4. Reconciliation results
    if batch_id:
        analyze_reconciliation_results(connection, batch_id)
    else:
        analyze_reconciliation_results(connection)
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

def main():
    parser = argparse.ArgumentParser(description="Analyze reconciliation results")
    parser.add_argument("--customer", help="Filter by customer name")
    parser.add_argument("--po", help="Filter by PO number")
    parser.add_argument("--batch-id", type=int, help="Analyze specific batch ID")
    parser.add_argument("--db-key", default="orders", help="Database key in config.yaml")
    parser.add_argument("--query", help="Run custom SQL query")
    
    args = parser.parse_args()
    
    # Get connection
    connection_string = get_connection_string(args.db_key)
    logger.info(f"Connecting to database...")
    
    try:
        connection = pyodbc.connect(connection_string)
        logger.info("Connected successfully")
        
        if args.query:
            # Run custom query
            df = execute_query(connection, args.query)
            print_results(df, "Custom Query Results")
        else:
            # Run full analysis
            create_analysis_report(connection, args.customer, args.po, args.batch_id)
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
