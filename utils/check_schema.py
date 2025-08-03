#!/usr/bin/env python3
"""
Check database schema and sample data
"""

import pyodbc
import yaml

def main():
    # Load config
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    conn_str = config['databases']['orders']['conn_str']

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        
        print('=== STAGING TABLE COLUMNS ===')
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'stg_fm_orders_shipped_table'
            ORDER BY ORDINAL_POSITION
        """)
        
        for row in cursor.fetchall():
            print(f'{row.COLUMN_NAME}: {row.DATA_TYPE} ({"NULL" if row.IS_NULLABLE == "YES" else "NOT NULL"})')
        
        print('\n=== ORDERS TABLE COLUMNS ===')
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'int_orders_extended'
            ORDER BY ORDINAL_POSITION
        """)
        
        for row in cursor.fetchall():
            print(f'{row.COLUMN_NAME}: {row.DATA_TYPE} ({"NULL" if row.IS_NULLABLE == "YES" else "NOT NULL"})')
            
        print('\n=== SAMPLE DATA FROM ORDERS ===')
        cursor.execute('SELECT TOP 3 * FROM int_orders_extended WHERE customer_name = \'GREYSON\' AND po_number = \'4755\'')
        for row in cursor.fetchall():
            print(row)
            
        print('\n=== SAMPLE DATA FROM STAGING ===')
        cursor.execute('SELECT TOP 3 * FROM stg_fm_orders_shipped_table WHERE customer_name = \'GREYSON\' AND po_number = \'4755\'')
        for row in cursor.fetchall():
            print(row)

if __name__ == "__main__":
    main()
