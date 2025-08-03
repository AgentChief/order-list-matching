#!/usr/bin/env python3
"""
TASK001 - Deploy Shipment Summary Cache Schema
Direct SQL execution approach
"""

import pyodbc

def get_connection_string():
    """Get connection string"""
    return "Driver={SQL Server};Server=ross-db-srv-test.database.windows.net;Database=ORDERS;UID=admin_ross;PWD=Active@IT2023;Encrypt=yes;TrustServerCertificate=yes;"

def deploy_schema():
    """Deploy the shipment summary cache schema and procedures"""
    
    connection_string = get_connection_string()
    
    print("🚀 TASK001 - DEPLOYING SHIPMENT SUMMARY CACHE SCHEMA")
    print("=" * 60)
    
    try:
        with pyodbc.connect(connection_string) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            
            print("1. Dropping existing table if exists...")
            drop_sql = "DROP TABLE IF EXISTS dbo.shipment_summary_cache;"
            cursor.execute(drop_sql)
            print("   ✅ Old table dropped")
            
            print("2. Creating cache table...")
            create_table_sql = """
            CREATE TABLE dbo.shipment_summary_cache (
                shipment_id INT NOT NULL PRIMARY KEY,
                customer_name NVARCHAR(100) NOT NULL,
                row_number INT NOT NULL,
                style_code NVARCHAR(50) NOT NULL,
                color_description NVARCHAR(100),
                delivery_method NVARCHAR(50),
                quantity INT NOT NULL,
                style_match_indicator CHAR(1) NOT NULL DEFAULT 'U',
                color_match_indicator CHAR(1) NOT NULL DEFAULT 'U',
                delivery_match_indicator CHAR(1) NOT NULL DEFAULT 'U',
                quantity_match_indicator CHAR(1) NOT NULL DEFAULT 'U',
                match_count INT NOT NULL DEFAULT 0,
                match_layers NVARCHAR(50),
                best_confidence DECIMAL(5,2) DEFAULT 0.00,
                avg_confidence DECIMAL(5,2) DEFAULT 0.00,
                total_matched_order_qty INT DEFAULT 0,
                quantity_variance INT DEFAULT 0,
                shipment_status NVARCHAR(20) NOT NULL DEFAULT 'UNKNOWN',
                outstanding_reviews INT DEFAULT 0,
                last_updated DATETIME2 DEFAULT GETDATE(),
                source_last_modified DATETIME2
            );
            """
            cursor.execute(create_table_sql)
            print("   ✅ Cache table created")
            
            print("3. Creating indexes...")
            
            indexes = [
                "CREATE NONCLUSTERED INDEX IX_customer_status ON dbo.shipment_summary_cache (customer_name, shipment_status);",
                "CREATE NONCLUSTERED INDEX IX_status_updated ON dbo.shipment_summary_cache (shipment_status, last_updated);",
                "CREATE NONCLUSTERED INDEX IX_confidence ON dbo.shipment_summary_cache (best_confidence DESC);",
                "CREATE NONCLUSTERED INDEX IX_indicators ON dbo.shipment_summary_cache (style_match_indicator, color_match_indicator, delivery_match_indicator, quantity_match_indicator);",
                "CREATE NONCLUSTERED INDEX IX_row_number ON dbo.shipment_summary_cache (row_number);"
            ]
            
            for i, index_sql in enumerate(indexes):
                cursor.execute(index_sql)
                print(f"   ✅ Index {i+1}/{len(indexes)} created")
            
            print("4. Creating lookup index...")
            lookup_index_sql = """
            CREATE NONCLUSTERED INDEX IX_shipment_summary_cache_customer_lookup 
            ON dbo.shipment_summary_cache (customer_name, shipment_status, row_number)
            INCLUDE (style_code, color_description, delivery_method, quantity, 
                     style_match_indicator, color_match_indicator, delivery_match_indicator, quantity_match_indicator,
                     best_confidence, quantity_variance);
            """
            cursor.execute(lookup_index_sql)
            print("   ✅ Lookup index created")
            
            print("5. Creating statistics view...")
            view_sql = """
            CREATE VIEW vw_shipment_summary_cache_stats AS
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT customer_name) as unique_customers,
                AVG(CAST(match_count AS FLOAT)) as avg_matches_per_shipment,
                SUM(CASE WHEN shipment_status = 'GOOD' THEN 1 ELSE 0 END) as good_count,
                SUM(CASE WHEN shipment_status = 'QUANTITY_ISSUES' THEN 1 ELSE 0 END) as quantity_issues_count,
                SUM(CASE WHEN shipment_status = 'DELIVERY_ISSUES' THEN 1 ELSE 0 END) as delivery_issues_count,
                SUM(CASE WHEN shipment_status = 'UNMATCHED' THEN 1 ELSE 0 END) as unmatched_count,
                SUM(CASE WHEN shipment_status = 'UNKNOWN' THEN 1 ELSE 0 END) as unknown_count,
                SUM(CASE WHEN style_match_indicator = 'Y' THEN 1 ELSE 0 END) as style_match_yes,
                SUM(CASE WHEN color_match_indicator = 'Y' THEN 1 ELSE 0 END) as color_match_yes,
                MAX(last_updated) as last_cache_refresh,
                MIN(last_updated) as oldest_cache_entry
            FROM dbo.shipment_summary_cache;
            """
            cursor.execute(view_sql)
            print("   ✅ Statistics view created")
            
            print("6. Verifying deployment...")
            
            # Check table exists
            table_check = cursor.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.shipment_summary_cache') AND type in (N'U')
            """).fetchone()[0]
            
            # Check view exists
            view_check = cursor.execute("""
                SELECT COUNT(*) FROM sys.objects 
                WHERE object_id = OBJECT_ID(N'dbo.vw_shipment_summary_cache_stats') AND type in (N'V')
            """).fetchone()[0]
            
            if table_check > 0 and view_check > 0:
                print("   ✅ All objects created successfully")
                print("\n✅ SCHEMA DEPLOYMENT SUCCESSFUL!")
                print("Next: Deploy stored procedure sp_refresh_shipment_summary_cache")
                return True
            else:
                print("   ❌ Some objects missing")
                return False
            
    except Exception as e:
        print(f"❌ Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    deploy_schema()
