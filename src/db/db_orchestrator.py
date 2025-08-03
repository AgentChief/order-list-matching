#!/usr/bin/env python3
import argparse
import os
import logging
from pathlib import Path
import pyodbc  # or another SQL Server connector like sqlalchemy
import yaml

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load database configuration from config.yaml
CONFIG_PATH = str(Path(__file__).parent / "config" / "config.yaml")

def load_config(path=CONFIG_PATH):
    """Load configuration from YAML file"""
    with open(path, "r") as f:
        config = yaml.safe_load(f)
        return config

def get_connection(db_key="orders"):
    """Get a database connection using the configuration"""
    connection_string = get_connection_string(db_key)
    logger.info(f"Connecting with: {connection_string}")
    return pyodbc.connect(connection_string)

def get_connection_string(db_key="orders"):
    """Get connection string from config file"""
    config = load_config()
    db_config = config.get('databases', {}).get(db_key, {})
    
    if 'conn_str' in db_config:
        return db_config['conn_str']
    
    # Fall back to parameters if conn_str not available
    return "Driver={SQL Server};Server=localhost;Database=order_matching;Trusted_Connection=yes;"

def run_migrations(connection):
    """Run all migration scripts to set up the database"""
    logger.info("Running database migrations...")
    
    migrations_dir = Path(__file__).parent / "db" / "migrations"
    
    # Get all SQL files in order
    migration_files = sorted([f for f in migrations_dir.glob("*.sql")])
    
    for migration_file in migration_files:
        logger.info(f"Running migration: {migration_file.name}")
        try:
            batches = read_sql_file(migration_file)
            for batch in batches:
                if batch.strip():
                    execute_sql(connection, batch)
        except Exception as e:
            logger.error(f"Migration {migration_file.name} failed: {e}")
            raise
    
    logger.info("All migrations completed successfully")

def read_sql_file(file_path):
    """Read SQL from file with proper handling of GO statements"""
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Split by GO statements to handle batch execution
    return [batch.strip() for batch in content.split('GO') if batch.strip()]

def execute_sql(connection, sql, params=None):
    """Execute SQL with proper error handling"""
    cursor = connection.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        connection.commit()
        
        # Try to fetch results only if we expect them (SELECT queries)
        if sql.strip().upper().startswith('SELECT'):
            try:
                rows = cursor.fetchall()
                # Convert rows to a list of tuples for easier handling
                return [tuple(row) for row in rows]
            except:
                # No results to fetch
                return []
        return cursor
    except Exception as e:
        connection.rollback()
        logger.error(f"SQL execution failed: {e}")
        logger.error(f"SQL statement: {sql}")
        raise

def create_staging_table(connection):
    """Execute 01_create_stg_fm_orders_shipped_table.sql"""
    logger.info("Creating staging table...")
    sql_file = Path("db/migrations/01_create_stg_fm_orders_shipped_table.sql")
    for batch in read_sql_file(sql_file):
        execute_sql(connection, batch)
    logger.info("Staging table created successfully")

def populate_staging_table(connection, customer=None, po=None, date_from=None, date_to=None):
    """Execute 02_populate_stg_fm_orders_shipped_table.sql with optional filters"""
    logger.info("Populating staging table...")
    
    # Read the base SQL script
    sql_file = Path("db/migrations/02_populate_stg_fm_orders_shipped_table.sql")
    base_sql = read_sql_file(sql_file)[0]  # Assuming the main insert is the first batch
    
    # Modify SQL with filters if provided
    if customer or po or date_from or date_to:
        where_clauses = []
        
        if customer:
            where_clauses.append(f"Customer = '{customer}'")
            
        if po:
            where_clauses.append(f"Customer_PO = '{po}'")
            
        if date_from:
            where_clauses.append(f"Shipped_Date >= '{date_from}'")
            
        if date_to:
            where_clauses.append(f"Shipped_Date <= '{date_to}'")
        
        # Add WHERE clause to SQL if we have filters
        if where_clauses:
            filter_clause = " AND ".join(where_clauses)
            # Insert the WHERE clause before the GROUP BY
            modified_sql = base_sql.replace("GROUP BY", f"WHERE {filter_clause} GROUP BY")
            execute_sql(connection, modified_sql)
        else:
            execute_sql(connection, base_sql)
    else:
        # Execute without modifications if no filters
        execute_sql(connection, base_sql)
    
    logger.info("Staging table populated successfully")

def create_reconciliation_tables(connection):
    """Execute 05_create_reconciliation_tables.sql"""
    logger.info("Creating reconciliation tables...")
    sql_file = Path("db/migrations/05_create_reconciliation_tables.sql")
    for batch in read_sql_file(sql_file):
        execute_sql(connection, batch)
    logger.info("Reconciliation tables created successfully")

def create_orders_extended_table(connection):
    """Execute 06_create_orders_extended_table.sql"""
    logger.info("Creating orders extended table...")
    sql_file = Path("db/migrations/06_create_orders_extended_table.sql")
    for batch in read_sql_file(sql_file):
        execute_sql(connection, batch)
    
    # Populate with sample data
    logger.info("Populating orders extended table with sample data...")
    sample_sql = "EXEC sp_populate_sample_orders"
    execute_sql(connection, sample_sql)
    
    logger.info("Orders extended table created and populated successfully")

def create_procedures(connection):
    """Execute 03 and 04 scripts to create stored procedures"""
    logger.info("Creating refresh procedure...")
    refresh_sql_file = Path("db/migrations/03_create_refresh_shipments_procedure.sql")
    for batch in read_sql_file(refresh_sql_file):
        execute_sql(connection, batch)
    
    logger.info("Creating batch reconciliation procedure...")
    reconcile_sql_file = Path("db/migrations/04_create_batch_reconciliation_procedure.sql")
    for batch in read_sql_file(reconcile_sql_file):
        execute_sql(connection, batch)
    
    logger.info("Stored procedures created successfully")

def run_enhanced_batch_reconciliation(connection, customer, po=None, quantity_tolerance=5.0, style_match_required=True, color_threshold=85.0):
    """Execute the enhanced batch reconciliation procedure with business-realistic parameters"""
    logger.info(f"Running enhanced batch reconciliation for {customer}{f' PO {po}' if po else ''}...")
    logger.info(f"Parameters: quantity_tolerance={quantity_tolerance}%, style_match_required={style_match_required}, color_threshold={color_threshold}%")
    
    # Run the enhanced procedure
    proc_sql = f"""
    EXEC sp_enhanced_batch_reconcile 
        @customer_name = '{customer}', 
        @po_number = {f"'{po}'" if po else "NULL"}, 
        @quantity_tolerance_percent = {quantity_tolerance},
        @style_match_required = {1 if style_match_required else 0},
        @color_confidence_threshold = {color_threshold},
        @batch_description = 'Enhanced matching from Python orchestrator - Phase 1'
    """
    result = execute_sql(connection, proc_sql)
    
    # The procedure returns a summary result set
    if result and len(result) > 0:
        batch_id, total_shipments, exact_matches, hitl_reviews, manual_reviews, auto_match_rate = result[0]
        logger.info(f"Enhanced reconciliation completed:")
        logger.info(f"  Batch ID: {batch_id}")
        logger.info(f"  Total Shipments: {total_shipments}")
        logger.info(f"  Exact Matches: {exact_matches}")
        logger.info(f"  HITL Reviews: {hitl_reviews}")
        logger.info(f"  Manual Reviews: {manual_reviews}")
        logger.info(f"  Auto Match Rate: {auto_match_rate}%")
        return batch_id
    else:
        logger.warning("No result returned from enhanced reconciliation procedure")
        return None

def analyze_delivery_methods(connection, batch_id=None):
    """Analyze delivery methods for mapping and insights"""
    logger.info(f"Analyzing delivery methods{f' for batch {batch_id}' if batch_id else ' (all data)'}...")
    
    proc_sql = f"""
    EXEC sp_map_delivery_methods 
        @batch_id = {batch_id if batch_id else 'NULL'}
    """
    result = execute_sql(connection, proc_sql)
    
    if result:
        logger.info("Delivery method analysis:")
        for row in result:
            method, count, customers, avg_qty, customer_list = row
            logger.info(f"  {method}: {count} shipments, {customers} customers, avg {avg_qty:.1f} qty - ({customer_list})")
    
    return result

def verify_match_preservation(connection, batch_id):
    """Test match preservation during refresh"""
    logger.info("Testing match preservation during refresh...")
    
    # Get current match counts
    before_sql = f"""
    SELECT 
        COUNT(*) as total_matches,
        SUM(CASE WHEN match_method = 'exact' THEN 1 ELSE 0 END) as exact_matches,
        SUM(CASE WHEN match_method = 'fuzzy' THEN 1 ELSE 0 END) as fuzzy_matches
    FROM reconciliation_result
    WHERE batch_id = {batch_id}
    """
    before_counts_result = execute_sql(connection, before_sql)
    before_counts = before_counts_result[0] if before_counts_result else (0, 0, 0)
    
    # Run the refresh procedure
    logger.info("Running data refresh procedure...")
    refresh_sql = "EXEC sp_refresh_stg_fm_orders_shipped"
    execute_sql(connection, refresh_sql)
    
    # Get post-refresh match counts
    after_sql = f"""
    SELECT 
        COUNT(*) as total_matches,
        SUM(CASE WHEN match_method = 'exact' THEN 1 ELSE 0 END) as exact_matches,
        SUM(CASE WHEN match_method = 'fuzzy' THEN 1 ELSE 0 END) as fuzzy_matches
    FROM reconciliation_result
    WHERE batch_id = {batch_id}
    """
    after_counts_result = execute_sql(connection, after_sql)
    after_counts = after_counts_result[0] if after_counts_result else (0, 0, 0)
    
    # Compare counts
    if before_counts[0] == after_counts[0]:
        logger.info("✅ Match preservation test PASSED: Total match count preserved")
    else:
        logger.warning(f"❌ Match preservation test FAILED: Before: {before_counts[0]}, After: {after_counts[0]}")
    
    if before_counts[1] == after_counts[1]:
        logger.info("✅ Exact match count preserved")
    else:
        logger.warning(f"❌ Exact match count changed: Before: {before_counts[1]}, After: {after_counts[1]}")
    
    if before_counts[2] == after_counts[2]:
        logger.info("✅ Fuzzy match count preserved")
    else:
        logger.warning(f"❌ Fuzzy match count changed: Before: {before_counts[2]}, After: {after_counts[2]}")
    
    return {
        'before': {
            'total': before_counts[0],
            'exact': before_counts[1],
            'fuzzy': before_counts[2]
        },
        'after': {
            'total': after_counts[0],
            'exact': after_counts[1],
            'fuzzy': after_counts[2]
        },
        'preserved': before_counts[0] == after_counts[0]
    }

def populate_orders_from_production(connection, customer=None, po=None, clear_existing=True):
    """Populate orders table from real production data via stg_order_list view"""
    logger.info(f"Populating orders from production data for {customer if customer else 'all customers'}{f' PO {po}' if po else ''}...")
    
    proc_sql = f"""
    EXEC sp_populate_orders_from_production 
        @customer_name = {f"'{customer}'" if customer else "NULL"}, 
        @po_number = {f"'{po}'" if po else "NULL"},
        @clear_existing = {1 if clear_existing else 0}
    """
    result = execute_sql(connection, proc_sql)
    
    if result and len(result) > 0:
        orders_inserted, customer_filter, po_filter = result[0]
        logger.info(f"Successfully populated {orders_inserted} orders from production data")
        return orders_inserted
    else:
        logger.warning("No result returned from order population procedure")
        return 0

def full_setup(connection, customer=None, po=None, date_from=None, date_to=None, threshold=85):
    """Run the full setup process"""
    try:
        # 1. Create all tables and procedures
        create_staging_table(connection)
        create_reconciliation_tables(connection)
        create_orders_extended_table(connection)
        create_procedures(connection)
        
        # 2. Populate staging table with shipment data
        populate_staging_table(connection, customer, po, date_from, date_to)
        
        # 3. Populate orders table from real production data
        if customer:
            populate_orders_from_production(connection, customer, po, clear_existing=True)
        
        # 4. Run reconciliation if customer specified
        if customer:
            batch_id = run_enhanced_batch_reconciliation(connection, customer, po, threshold)
            
            # 5. Test match preservation
            result = verify_match_preservation(connection, batch_id)
            
            logger.info("\n=== SUMMARY ===")
            logger.info(f"Total matches: {result['before']['total']}")
            logger.info(f"Exact matches: {result['before']['exact']}")
            logger.info(f"Fuzzy matches: {result['before']['fuzzy']}")
            logger.info(f"Match preservation: {'✅ PASSED' if result['preserved'] else '❌ FAILED'}")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Set up and test the reconciliation system")
    parser.add_argument("--database", default="order_matching", help="Database name (default: order_matching)")
    parser.add_argument("--db-key", default="orders", help="Database key in config.yaml (default: orders)")
    parser.add_argument("--customer", help="Filter by customer name")
    parser.add_argument("--po", help="Filter by PO number")
    parser.add_argument("--date-from", help="Start date for shipments (YYYY-MM-DD)")
    parser.add_argument("--date-to", help="End date for shipments (YYYY-MM-DD)")
    parser.add_argument("--threshold", type=int, default=85, help="Similarity threshold for fuzzy matching (default: 85)")
    parser.add_argument("--setup-only", action="store_true", help="Only create tables and procedures, don't run reconciliation")
    
    args = parser.parse_args()
    
    # Get connection string from config
    connection_string = get_connection_string(args.db_key)
    
    # Override database name if specified
    if args.database != "order_matching":
        connection_string = connection_string.replace("Database=ORDERS", f"Database={args.database}")
    
    logger.info(f"Using connection string: {connection_string}")
    
    try:
        connection = pyodbc.connect(connection_string)
        logger.info("Connected successfully")
        
        if args.setup_only:
            # Only create tables and procedures
            create_staging_table(connection)
            create_reconciliation_tables(connection)
            create_procedures(connection)
        else:
            # Run the full process
            full_setup(
                connection, 
                args.customer, 
                args.po, 
                args.date_from, 
                args.date_to, 
                args.threshold
            )
        
        connection.close()
        logger.info("Process completed successfully")
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
