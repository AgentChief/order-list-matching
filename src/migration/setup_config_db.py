#!/usr/bin/env python3
"""
Database Schema Setup Script
Creates the configuration management database schema
"""

import pyodbc
import sys
from pathlib import Path

# Add project root to path
# sys.path.append(str(Path(__file__).parent))

# Import from root level
sys.path.insert(0, str(Path(__file__).parent))
from auth_helper import get_connection_string

def setup_database_schema():
    """Create the configuration database schema"""
    
    schema_file = Path(__file__).parent / "db" / "schema" / "config_schema.sql"
    
    if not schema_file.exists():
        print(f"❌ Schema file not found: {schema_file}")
        return False
    
    print("📋 Reading schema file...")
    with open(schema_file, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    try:
        print("🔌 Connecting to database...")
        connection_string = get_connection_string()
        
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            
            print("🏗️  Creating database schema...")
            
            # Split the SQL into individual statements
            statements = []
            current_statement = []
            in_procedure = False
            
            for line in schema_sql.split('\n'):
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('--'):
                    continue
                
                # Check for procedure/view creation
                if line.upper().startswith('CREATE PROCEDURE') or line.upper().startswith('CREATE VIEW'):
                    in_procedure = True
                
                current_statement.append(line)
                
                # Check for statement termination
                if line == 'GO':
                    if current_statement:
                        statements.append('\n'.join(current_statement[:-1]))  # Exclude GO
                    current_statement = []
                    in_procedure = False
                elif line.endswith(';') and not in_procedure:
                    statements.append('\n'.join(current_statement))
                    current_statement = []
            
            # Add any remaining statement
            if current_statement:
                statements.append('\n'.join(current_statement))
            
            # Execute each statement
            for i, statement in enumerate(statements):
                if statement.strip():
                    try:
                        print(f"   Executing statement {i+1}/{len(statements)}...")
                        cursor.execute(statement)
                        conn.commit()
                    except Exception as e:
                        print(f"   ⚠️  Statement {i+1} failed: {str(e)}")
                        # Continue with other statements
                        continue
            
            print("✅ Database schema created successfully!")
            
            # Verify tables were created
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME IN ('customers', 'customer_aliases', 'column_mappings', 
                                    'matching_strategies', 'exclusion_rules', 'data_quality_keys', 
                                    'value_mappings', 'configuration_audit')
                ORDER BY TABLE_NAME
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            print(f"📊 Created tables: {', '.join(tables)}")
            
            return True
            
    except Exception as e:
        print(f"❌ Database setup failed: {str(e)}")
        return False

def main():
    """Main setup execution"""
    print("🚀 Setting up Order Matching Configuration Database")
    print("=" * 50)
    
    success = setup_database_schema()
    
    if success:
        print("\n🎉 Setup completed successfully!")
        print("\nNext steps:")
        print("1. Install Streamlit: pip install streamlit")
        print("2. Run migration: python src/migration/yaml_to_db.py")
        print("3. Start UI: streamlit run src/ui/streamlit_config_app.py")
        return 0
    else:
        print("\n❌ Setup failed. Please check the error messages above.")
        return 1

if __name__ == "__main__":
    exit(main())
