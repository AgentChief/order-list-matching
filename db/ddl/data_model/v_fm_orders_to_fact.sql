-- V_FM_Orders_To_FACT View
-- Provides a mapping between original FM_orders_shipped and FACT_Orders_Shipped

CREATE OR ALTER VIEW [dbo].[V_FM_Orders_To_FACT] AS
SELECT
    fs.[shipment_id],
    fm.[id] AS original_id,
    fs.[customer_name],
    fs.[po_number],
    fs.[style_code],
    fs.[color_description],
    fs.[delivery_method],
    fs.[shipped_date],
    fs.[quantity],
    fs.[size],
    fs.[reconciliation_status],
    fs.[reconciliation_id],
    fs.[reconciliation_date]
FROM
    [dbo].[FACT_Orders_Shipped] fs
JOIN
    [dbo].[FM_orders_shipped] fm ON fs.[source_shipment_id] = fm.[id];
