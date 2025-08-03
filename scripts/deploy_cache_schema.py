#!/usr/bin/env python3
"""
TASK001 - Deploy Shipment Summary Cache Schema
Deploys the cache table and stored procedures to the database
"""

import sys
import pyodbc
from pathlib import Path

def get_connection_string(db_key="orders"):
    """Get connection string from config file"""
    # Fall back to default connection string for this deployment
    return "Driver={SQL Server};Server=ross-db-srv-test.database.windows.net;Database=ORDERS;UID=admin_ross;PWD=Active@IT2023;Encrypt=yes;TrustServerCertificate=yes;"

def deploy_schema():
    """Deploy the shipment summary cache schema and procedures"""
    
    # Get project root
    project_root = Path(__file__).parent.parent
    
    connection_string = get_connection_string()
    
    print("🚀 TASK001 - DEPLOYING SHIPMENT SUMMARY CACHE SCHEMA")
    print("=" * 60)
    
    # Read SQL files
    schema_file = project_root / "db" / "schema" / "shipment_summary_cache_simple.sql"
    procedure_file = project_root / "db" / "procedures" / "sp_refresh_shipment_summary_cache.sql"
    
    if not schema_file.exists():
        print(f"❌ Schema file not found: {schema_file}")
        return False
        
    if not procedure_file.exists():
        print(f"❌ Procedure file not found: {procedure_file}")
        return False
    
    try:
        with pyodbc.connect(connection_string) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Deploy schema
            print("1. Deploying cache table schema...")
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            # Split on GO statements and execute each batch
            batches = [batch.strip() for batch in schema_sql.split('GO') if batch.strip()]
            
            for i, batch in enumerate(batches):
                if batch and not batch.startswith('--') and len(batch.replace('-', '').replace('\n', '').strip()) > 0:
                    # Skip pure comment blocks
                    has_sql = any(line.strip() and not line.strip().startswith('--') for line in batch.split('\n'))
                    if has_sql:
                        print(f"   Executing batch {i+1}/{len(batches)}...")
                        print(f"   SQL: {batch[:100].replace(chr(10), ' ')}...")  # Debug: show first 100 chars
                        cursor.execute(batch)
            
            print("   ✅ Cache table schema deployed successfully")
            
            # Deploy stored procedure
            print("2. Deploying stored procedure...")
            with open(procedure_file, 'r') as f:
                procedure_sql = f.read()
            
            # Split on GO statements and execute each batch
            proc_batches = [batch.strip() for batch in procedure_sql.split('GO') if batch.strip()]
            
            for i, batch in enumerate(proc_batches):
                if batch:
                    print(f"   Executing batch {i+1}/{len(proc_batches)}...")
                    cursor.execute(batch)
            
            print("   ✅ Stored procedure deployed successfully")
            
            # Verify deployment
            print("3. Verifying deployment...")
            
            # Check table exists
            table_check = cursor.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.shipment_summary_cache') AND type in (N'U')
            """).fetchone()[0]
            
            if table_check > 0:
                print("   ✅ Cache table exists")
            else:
                print("   ❌ Cache table not found")
                return False
            
            # Check stored procedure exists
            proc_check = cursor.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.sp_refresh_shipment_summary_cache') AND type in (N'P')
            """).fetchone()[0]
            
            if proc_check > 0:
                print("   ✅ Stored procedure exists")
            else:
                print("   ❌ Stored procedure not found")
                return False
            
            # Check view exists
            view_check = cursor.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.vw_shipment_summary_cache_stats') AND type in (N'V')
            """).fetchone()[0]
            
            if view_check > 0:
                print("   ✅ Statistics view exists")
            else:
                print("   ❌ Statistics view not found")
                return False
            
            print("\n✅ DEPLOYMENT SUCCESSFUL!")
            print("Ready to run test_summary_cache.py")
            return True
            
    except Exception as e:
        print(f"❌ Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = deploy_schema()
    sys.exit(0 if success else 1)
