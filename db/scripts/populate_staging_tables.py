#!/usr/bin/env python3
"""
Populate Staging Tables Script
Hydrates stg_fm_orders_shipped_table from FM_orders_shipped
"""

from db_orchestrator import get_connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def populate_staging_shipments():
    """Populate the staging shipments table from FM_orders_shipped"""
    connection = get_connection()
    cursor = connection.cursor()
    
    try:
        logger.info("Truncating existing staging data...")
        cursor.execute("TRUNCATE TABLE [dbo].[stg_fm_orders_shipped_table]")
        
        logger.info("Populating staging table from FM_orders_shipped...")
        insert_sql = """
        INSERT INTO [dbo].[stg_fm_orders_shipped_table] (
            [customer_name],
            [po_number],
            [style_code],
            [color_description],
            [delivery_method],
            [shipped_date],
            [quantity],
            [reconciliation_status],
            [created_at],
            [updated_at],
            [last_sync_date]
        )
        SELECT
            [Customer] AS customer_name,
            [Customer_PO] AS po_number,
            [Style] AS style_code,
            [Color] AS color_description,
            [Shipping_Method] AS delivery_method,
            [Shipped_Date] AS shipped_date,
            SUM([Qty]) AS quantity,
            'UNMATCHED' AS reconciliation_status,
            GETDATE() AS created_at,
            GETDATE() AS updated_at,
            GETDATE() AS last_sync_date
        FROM [dbo].[FM_orders_shipped]
        GROUP BY
            [Customer],
            [Customer_PO],
            [Style],
            [Color],
            [Shipping_Method],
            [Shipped_Date]
        """
        
        cursor.execute(insert_sql)
        connection.commit()
        
        # Check results
        cursor.execute("SELECT COUNT(*) FROM [dbo].[stg_fm_orders_shipped_table]")
        total_count = cursor.fetchone()[0]
        logger.info(f"✅ Populated staging table with {total_count:,} aggregated shipment records")
        
        # Check GREYSON PO 4755 specifically
        cursor.execute("""
            SELECT COUNT(*) FROM [dbo].[stg_fm_orders_shipped_table]
            WHERE customer_name = 'GREYSON' AND po_number = 4755
        """)
        greyson_count = cursor.fetchone()[0]
        logger.info(f"✅ GREYSON PO 4755: {greyson_count} aggregated shipment records")
        
        if greyson_count > 0:
            cursor.execute("""
                SELECT TOP 5 shipment_id, style_code, color_description, quantity
                FROM [dbo].[stg_fm_orders_shipped_table]
                WHERE customer_name = 'GREYSON' AND po_number = 4755
            """)
            samples = cursor.fetchall()
            logger.info("Sample GREYSON PO 4755 records:")
            for row in samples:
                logger.info(f"  ID: {row[0]} | Style: {row[1]} | Color: {row[2]} | Qty: {row[3]}")
        
    except Exception as e:
        logger.error(f"Failed to populate staging table: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    populate_staging_shipments()
