-- fact_orders_shipped.sql
-- Final fact table for shipped orders
-- This is the mart layer representation that combines raw data with business logic

CREATE OR ALTER VIEW [dbo].[mart_fact_orders_shipped] AS
SELECT
    shipment_id,
    customer_name,
    po_number,
    style_code,
    color_description,
    delivery_method,
    shipped_date,
    quantity,
    size,
    order_reference_id,
    alias_related_item,
    original_alias_related_item,
    pattern_id,
    customer_alt_po,
    shipping_country,
    reconciliation_status,
    reconciliation_id,
    reconciliation_date,
    split_shipment,
    split_group_id,
    parent_shipment_id,
    last_reviewed_by,
    last_reviewed_date,
    created_at,
    updated_at,
    style_color_key
FROM 
    [dbo].[int_shipments_extended];
