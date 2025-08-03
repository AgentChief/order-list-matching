"""
Debug database structure and data availability
"""
import pyodbc
from auth_helper import get_connection_string

def debug_database():
    conn = pyodbc.connect(get_connection_string())
    cursor = conn.cursor()
    
    print("=== Database Structure Debug ===")
    
    # Check what tables exist
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Available tables: {tables}")
    
    # Check if we have any orders data
    if 'stg_order_list' in tables:
        cursor.execute("SELECT COUNT(*) FROM stg_order_list WHERE customer_name = 'GREYSON'")
        count = cursor.fetchone()[0]
        print(f"GREYSON orders in stg_order_list: {count}")
        
        # Check column names
        cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stg_order_list'")
        columns = [row[0] for row in cursor.fetchall()]
        print(f"stg_order_list columns: {columns}")
        
        # Sample data
        cursor.execute("SELECT TOP 5 * FROM stg_order_list WHERE customer_name = 'GREYSON'")
        rows = cursor.fetchall()
        print(f"Sample GREYSON orders: {len(rows)} rows")
        if rows:
            print("First row:", dict(zip(columns, rows[0])))
    else:
        print("❌ stg_order_list table not found!")
        
        # Check for similar table names
        order_tables = [t for t in tables if 'order' in t.lower()]
        print(f"Tables containing 'order': {order_tables}")
    
    # Check shipments table
    if 'stg_fm_orders_shipped_table' in tables:
        cursor.execute("SELECT COUNT(*) FROM stg_fm_orders_shipped_table WHERE customer_name = 'GREYSON'")
        count = cursor.fetchone()[0]
        print(f"GREYSON shipments in stg_fm_orders_shipped_table: {count}")
    else:
        print("❌ stg_fm_orders_shipped_table table not found!")
        
        # Check for similar table names
        ship_tables = [t for t in tables if 'ship' in t.lower()]
        print(f"Tables containing 'ship': {ship_tables}")
    
    conn.close()

if __name__ == "__main__":
    debug_database()
