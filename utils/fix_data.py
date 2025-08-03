#!/usr/bin/env python3
"""
Quick script to update GREYSON PO 4755 order data with realistic matching records
"""

import pyodbc
import yaml
import os

def main():
    try:
        # Load database config
        with open('config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        conn_str = config['databases']['orders']['conn_str']
        
        # Read SQL file
        with open('fix_greyson_data.sql', 'r') as f:
            sql_content = f.read()
        
        # Connect and execute
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # Split and execute statements
            statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
            
            for statement in statements:
                if statement:
                    print(f"Executing: {statement[:50]}...")
                    cursor.execute(statement)
            
            conn.commit()
            print("\n✅ Successfully updated GREYSON PO 4755 order data")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
