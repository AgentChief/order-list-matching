-- populate_stg_fm_orders_shipped_table.sql
-- Script to populate the staging table with aggregated data from FM_orders_shipped

-- Clear existing data
TRUNCATE TABLE [dbo].[stg_fm_orders_shipped_table];

-- Insert aggregated data from FM_orders_shipped
INSERT INTO [dbo].[stg_fm_orders_shipped_table] (
    [representative_shipment_id],
    [customer_name],
    [po_number],
    [style_code],
    [color_description],
    [delivery_method],
    [shipped_date],
    [quantity],
    [size_breakdown],
    [reconciliation_status],
    [reconciliation_id],
    [reconciliation_date],
    [split_shipment],
    [split_group_id],
    [parent_shipment_id],
    [last_reviewed_by],
    [last_reviewed_date],
    [created_at],
    [updated_at],
    [last_sync_date]
)
SELECT
    MIN([id]) AS representative_shipment_id,
    [Customer] AS customer_name,
    [Customer_PO] AS po_number,
    [Style] AS style_code,
    [Color] AS color_description,
    [Shipping_Method] AS delivery_method,
    [Shipped_Date] AS shipped_date,
    SUM([Qty]) AS quantity,
    STRING_AGG([Size] + '(' + CAST([Qty] AS VARCHAR) + ')', ', ') AS size_breakdown,
    MAX([reconciliation_status]) AS reconciliation_status,
    MAX([reconciliation_id]) AS reconciliation_id,
    MAX([reconciliation_date]) AS reconciliation_date,
    CAST(MAX(CAST([split_shipment] AS INT)) AS BIT) AS split_shipment,
    [split_group_id],
    MAX([parent_shipment_id]) AS parent_shipment_id,
    MAX([last_reviewed_by]) AS last_reviewed_by,
    MAX([last_reviewed_date]) AS last_reviewed_date,
    MIN([created_at]) AS created_at,
    MAX([updated_at]) AS updated_at,
    GETDATE() AS last_sync_date
FROM 
    [dbo].[FM_orders_shipped]
GROUP BY
    [Customer],
    [Customer_PO],
    [Style],
    [Color],
    [Shipping_Method],
    [Shipped_Date],
    [split_group_id];

PRINT 'Populated stg_fm_orders_shipped_table with aggregated data.';
