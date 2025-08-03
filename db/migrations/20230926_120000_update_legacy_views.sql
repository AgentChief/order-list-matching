-- Migration script for updating existing views to reference new models
-- 20230926_120000_update_legacy_views.sql

-- Update any existing views that reference the old tables directly
-- This ensures backward compatibility while transitioning to the new structure

-- Update v_fact_order_list_canonical if it exists
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_fact_order_list_canonical')
BEGIN
    EXEC('
    ALTER VIEW [dbo].[v_fact_order_list_canonical] AS
    SELECT *
    FROM [dbo].[mart_fact_order_list]
    ');
    
    PRINT 'Updated view: v_fact_order_list_canonical to reference mart_fact_order_list';
END
ELSE
BEGIN
    PRINT 'View v_fact_order_list_canonical does not exist, skipping';
END;

-- Update v_fm_orders_to_fact if it exists
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_fm_orders_to_fact')
BEGIN
    EXEC('
    ALTER VIEW [dbo].[v_fm_orders_to_fact] AS
    SELECT *
    FROM [dbo].[mart_fact_orders_shipped]
    ');
    
    PRINT 'Updated view: v_fm_orders_to_fact to reference mart_fact_orders_shipped';
END
ELSE
BEGIN
    PRINT 'View v_fm_orders_to_fact does not exist, skipping';
END;

-- Update v_order_shipment_reconciliation if it exists
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_order_shipment_reconciliation')
BEGIN
    EXEC('
    ALTER VIEW [dbo].[v_order_shipment_reconciliation] AS
    SELECT *
    FROM [dbo].[mart_reconciliation_summary]
    ');
    
    PRINT 'Updated view: v_order_shipment_reconciliation to reference mart_reconciliation_summary';
END
ELSE
BEGIN
    PRINT 'View v_order_shipment_reconciliation does not exist, skipping';
END;

PRINT 'Legacy view migration complete. All views now reference the new dbt-style models.';
