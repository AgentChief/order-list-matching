"""
Setup HITL (Human-in-the-Loop) database tables
Run this script to create the necessary tables for value mappings and manual review decisions
"""

import pyodbc
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from auth_helper import get_connection_string
from pathlib import Path

def setup_hitl_tables():
    """Create HITL support tables in the database"""
    
    # Read the SQL script
    sql_file = Path("db/migrations/08_create_hitl_tables.sql")
    
    if not sql_file.exists():
        print(f"Error: SQL file not found at {sql_file}")
        return False
    
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    try:
        # Get database connection
        connection_string = get_connection_string()
        
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            
            # Split the SQL by GO statements (batch separator)
            batches = sql_content.split('GO')
            
            for i, batch in enumerate(batches):
                batch = batch.strip()
                if batch:
                    try:
                        print(f"Executing batch {i+1}/{len(batches)}...")
                        cursor.execute(batch)
                        conn.commit()
                        print(f"✅ Batch {i+1} completed successfully")
                    except Exception as e:
                        print(f"❌ Error in batch {i+1}: {str(e)}")
                        # Continue with other batches
                        continue
            
            print("\n🎉 HITL database setup completed!")
            return True
            
    except Exception as e:
        print(f"❌ Database connection error: {str(e)}")
        return False

def verify_hitl_tables():
    """Verify that HITL tables were created successfully"""
    
    tables_to_check = [
        'value_mappings',
        'hitl_decisions', 
        'mapping_usage_log'
    ]
    
    try:
        connection_string = get_connection_string()
        
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            
            print("\n🔍 Verifying HITL tables...")
            
            for table in tables_to_check:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = '{table}'
                """)
                
                count = cursor.fetchone()[0]
                status = "✅ EXISTS" if count > 0 else "❌ MISSING"
                print(f"{table:20} {status}")
            
            # Check views
            views_to_check = ['v_hitl_summary', 'v_mapping_effectiveness']
            
            print("\n🔍 Verifying HITL views...")
            
            for view in views_to_check:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.VIEWS
                    WHERE TABLE_NAME = '{view}'
                """)
                
                count = cursor.fetchone()[0]
                status = "✅ EXISTS" if count > 0 else "❌ MISSING"
                print(f"{view:20} {status}")
            
            # Check stored procedure
            cursor.execute("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_NAME = 'sp_apply_value_mapping'
                AND ROUTINE_TYPE = 'PROCEDURE'
            """)
            
            count = cursor.fetchone()[0]
            status = "✅ EXISTS" if count > 0 else "❌ MISSING"
            print(f"{'sp_apply_value_mapping':20} {status}")
            
            return True
            
    except Exception as e:
        print(f"❌ Verification error: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Setting up HITL (Human-in-the-Loop) database tables...")
    
    success = setup_hitl_tables()
    
    if success:
        verify_hitl_tables()
        print("\n✨ Setup complete! You can now use the HITL interface in Streamlit.")
    else:
        print("\n💥 Setup failed. Please check the error messages above.")
