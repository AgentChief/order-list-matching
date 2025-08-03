-- Migration script for new dbt-style structure
-- 20230925_120000_dbt_structure_migration.sql

-- Create views for staging models
IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'stg_order_list')
BEGIN
    EXEC('
    CREATE VIEW [dbo].[stg_order_list] AS
    SELECT
        [record_uuid] AS order_id,
        [CUSTOMER NAME] AS customer_name,
        [PO NUMBER] AS po_number,
        [PLANNED DELIVERY METHOD] AS delivery_method,
        [CUSTOMER STYLE] AS style_code,
        [CUSTOMER COLOUR DESCRIPTION] AS color_description,
        [CUSTOMER''S COLOUR CODE (CUSTOM FIELD) CUSTOMER PROVIDES THIS] AS customer_color_code,
        [AAG ORDER NUMBER] AS aag_order_number,
        [ORDER TYPE] AS order_type,
        [ORDER DATE PO RECEIVED] AS order_date,
        [TOTAL QTY] AS quantity,
        [created_at],
        [updated_at]
    FROM
        [dbo].[FACT_ORDER_LIST]
    ');
    
    PRINT 'Created view: stg_order_list';
END
ELSE
BEGIN
    PRINT 'View stg_order_list already exists';
END;

IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'stg_fm_orders_shipped')
BEGIN
    EXEC('
    CREATE VIEW [dbo].[stg_fm_orders_shipped] AS
    SELECT
        Ship_ID AS shipment_id,
        Ship_Number AS shipment_number,
        Customer AS customer_name,
        PO_Number AS po_number,
        Style AS style_code,
        Color AS color_code,
        Size AS size_code,
        Quantity AS quantity,
        Unit_Price AS unit_price,
        Total_Amount AS total_amount,
        Status AS status,
        Ship_Date AS ship_date,
        Updated_Date AS updated_date,
        Order_Reference_ID AS order_reference_id,
        Reconciliation_Status AS reconciliation_status,
        Split_Shipment AS split_shipment
    FROM
        [dbo].[FM_orders_shipped]
    WHERE
        Deleted = 0 OR Deleted IS NULL
    ');
    
    PRINT 'Created view: stg_fm_orders_shipped';
END
ELSE
BEGIN
    PRINT 'View stg_fm_orders_shipped already exists';
END;

-- Create views for intermediate models
IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'int_orders_extended')
BEGIN
    EXEC('
    CREATE VIEW [dbo].[int_orders_extended] AS
    SELECT
        o.*,
        CONCAT(o.customer_name, ''_'', o.order_number, ''_'', o.style_code, ''_'', o.color_code) AS order_key,
        CASE 
            WHEN o.status = ''Canceled'' THEN ''CANCELED''
            WHEN o.status = ''Completed'' THEN ''COMPLETED''
            ELSE ''ACTIVE''
        END AS order_status_normalized
    FROM
        [dbo].[stg_order_list] o
    ');
    
    PRINT 'Created view: int_orders_extended';
END
ELSE
BEGIN
    PRINT 'View int_orders_extended already exists';
END;

IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'int_shipments_extended')
BEGIN
    EXEC('
    CREATE VIEW [dbo].[int_shipments_extended] AS
    SELECT
        s.*,
        CONCAT(s.customer_name, ''_'', s.po_number, ''_'', s.style_code, ''_'', s.color_code) AS shipment_key,
        CASE 
            WHEN s.status = ''Canceled'' THEN ''CANCELED''
            WHEN s.status = ''Shipped'' THEN ''SHIPPED''
            ELSE ''PROCESSING''
        END AS shipment_status_normalized
    FROM
        [dbo].[stg_fm_orders_shipped] s
    ');
    
    PRINT 'Created view: int_shipments_extended';
END
ELSE
BEGIN
    PRINT 'View int_shipments_extended already exists';
END;

-- Create views for mart models
IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'mart_fact_order_list')
BEGIN
    EXEC('
    CREATE VIEW [dbo].[mart_fact_order_list] AS
    SELECT
        o.order_number AS order_id,
        o.customer_name,
        o.style_code,
        o.color_code,
        o.size_code,
        o.quantity,
        o.unit_price,
        o.total_amount,
        o.order_status_normalized AS status,
        o.order_date,
        o.updated_date,
        o.order_key
    FROM
        [dbo].[int_orders_extended] o
    ');
    
    PRINT 'Created view: mart_fact_order_list';
END
ELSE
BEGIN
    PRINT 'View mart_fact_order_list already exists';
END;

IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'mart_fact_orders_shipped')
BEGIN
    EXEC('
    CREATE VIEW [dbo].[mart_fact_orders_shipped] AS
    SELECT
        s.shipment_id,
        s.customer_name,
        s.po_number,
        s.style_code,
        s.color_code,
        s.size_code,
        s.quantity,
        s.unit_price,
        s.total_amount,
        s.shipment_status_normalized AS status,
        s.ship_date,
        s.updated_date,
        s.order_reference_id,
        s.reconciliation_status,
        s.split_shipment,
        s.shipment_key
    FROM
        [dbo].[int_shipments_extended] s
    ');
    
    PRINT 'Created view: mart_fact_orders_shipped';
END
ELSE
BEGIN
    PRINT 'View mart_fact_orders_shipped already exists';
END;

IF NOT EXISTS (SELECT * FROM sys.views WHERE name = 'mart_reconciliation_summary')
BEGIN
    EXEC('
    CREATE VIEW [dbo].[mart_reconciliation_summary] AS
    SELECT
        s.shipment_id,
        s.customer_name,
        s.po_number,
        s.style_code,
        s.color_code,
        s.size_code,
        s.quantity AS shipped_quantity,
        s.ship_date,
        s.reconciliation_status,
        o.order_id,
        o.quantity AS ordered_quantity,
        o.order_date,
        CASE
            WHEN s.reconciliation_status = ''MATCHED'' THEN ''YES''
            ELSE ''NO''
        END AS is_reconciled,
        DATEDIFF(day, o.order_date, s.ship_date) AS days_to_ship
    FROM
        [dbo].[mart_fact_orders_shipped] s
    LEFT JOIN
        [dbo].[mart_fact_order_list] o ON s.order_reference_id = o.order_id
    ');
    
    PRINT 'Created view: mart_reconciliation_summary';
END
ELSE
BEGIN
    PRINT 'View mart_reconciliation_summary already exists';
END;

-- Move stored procedures to procedures directory
-- Note: This is a logical move only - SQL Server doesn't have physical directories for procedures
-- The actual stored procedures will remain in the database with their original names
PRINT 'Note: Stored procedures remain in the database with their original names.';
PRINT 'The procedures directory structure is for source control organization only.';

-- Create test procedure for data quality
IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'test_reconciliation_consistency')
BEGIN
    EXEC('
    CREATE PROCEDURE [dbo].[test_reconciliation_consistency]
    AS
    BEGIN
        SET NOCOUNT ON;
        
        -- Test 1: Check for shipments with reconciliation_status = ''MATCHED'' but no order_reference_id
        SELECT
            ''Inconsistent match status'' AS test_name,
            COUNT(*) AS failure_count,
            ''Shipments marked as MATCHED but missing order_reference_id'' AS description
        FROM
            [dbo].[FACT_Orders_Shipped]
        WHERE
            reconciliation_status = ''MATCHED''
            AND order_reference_id IS NULL;
        
        -- Test 2: Check for orders referenced by multiple shipments (may be valid for split shipments)
        SELECT
            ''Multiple shipments per order'' AS test_name,
            order_reference_id,
            COUNT(*) AS shipment_count,
            ''Order referenced by multiple shipments'' AS description
        FROM
            [dbo].[FACT_Orders_Shipped]
        WHERE
            order_reference_id IS NOT NULL
        GROUP BY
            order_reference_id
        HAVING
            COUNT(*) > 1;
        
        -- Test 3: Check for quantity mismatches between orders and shipments
        SELECT
            ''Quantity mismatch'' AS test_name,
            o.order_id,
            s.shipment_id,
            o.quantity AS order_quantity,
            s.quantity AS shipment_quantity,
            ABS(o.quantity - s.quantity) AS quantity_difference,
            ''Order and shipment quantities do not match'' AS description
        FROM
            [dbo].[mart_fact_order_list] o
        JOIN
            [dbo].[mart_fact_orders_shipped] s ON o.order_id = s.order_reference_id
        WHERE
            o.quantity <> s.quantity
            AND s.split_shipment = 0; -- Exclude split shipments which may have partial quantities
        
        RETURN 0;
    END
    ');
    
    PRINT 'Created procedure: test_reconciliation_consistency';
END
ELSE
BEGIN
    PRINT 'Procedure test_reconciliation_consistency already exists';
END;

PRINT 'Migration to dbt-style structure complete.';
