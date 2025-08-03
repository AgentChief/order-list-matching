-- 02_populate_stg_fm_orders_shipped_table.sql
-- Populates the staging table with aggregated data from FM_orders_shipped
-- Modified to handle missing columns in the source table

-- Clear existing data
IF OBJECT_ID('dbo.stg_fm_orders_shipped_table', 'U') IS NOT NULL
BEGIN
    TRUNCATE TABLE [dbo].[stg_fm_orders_shipped_table];
END
ELSE
BEGIN
    PRINT 'Error: stg_fm_orders_shipped_table does not exist. Please run 01_create_stg_fm_orders_shipped_table.sql first.';
    RETURN;
END

-- Insert aggregated data from FM_orders_shipped using a different approach
-- First, create a temporary table with row numbers
SELECT 
    ROW_NUMBER() OVER (ORDER BY [Customer], [Customer_PO], [Style], [Color]) AS temp_id,
    [Customer],
    [Customer_PO],
    [Style],
    [Color],
    [Shipping_Method],
    [Shipped_Date],
    [Size],
    [Qty]
INTO #temp_shipments
FROM 
    [dbo].[FM_orders_shipped];

-- Now aggregate from the temp table which has a guaranteed ID column
INSERT INTO [dbo].[stg_fm_orders_shipped_table] (
    [source_shipment_id],
    [customer_name],
    [po_number],
    [style_code],
    [color_description],
    [delivery_method],
    [shipped_date],
    [quantity],
    [size_breakdown]
)
SELECT
    MIN(temp_id) AS source_shipment_id,
    [Customer] AS customer_name,
    [Customer_PO] AS po_number,
    [Style] AS style_code,
    [Color] AS color_description,
    [Shipping_Method] AS delivery_method,
    [Shipped_Date] AS shipped_date,
    SUM([Qty]) AS quantity,
    STRING_AGG([Size] + '(' + CAST([Qty] AS VARCHAR) + ')', ', ') AS size_breakdown
FROM 
    #temp_shipments
GROUP BY
    [Customer],
    [Customer_PO],
    [Style],
    [Color],
    [Shipping_Method],
    [Shipped_Date];

-- Clean up the temp table
DROP TABLE #temp_shipments;

-- Set default values for reconciliation status
UPDATE [dbo].[stg_fm_orders_shipped_table]
SET 
    [reconciliation_status] = 'UNMATCHED',
    [created_at] = GETDATE(),
    [updated_at] = GETDATE(),
    [last_sync_date] = GETDATE();

PRINT 'Populated stg_fm_orders_shipped_table with aggregated data.';
PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows updated with default values.';
