-- fact_order_list.sql
-- Final fact table for order list
-- This is the mart layer representation for orders

CREATE OR ALTER VIEW [dbo].[mart_fact_order_list] AS
SELECT
    order_id,
    customer_name,
    po_number,
    delivery_method,
    style_code,
    color_description,
    customer_color_code,
    aag_order_number,
    order_type,
    order_date,
    quantity,
    -- size field removed as it's not in the source
    alias_related_item,
    original_alias_related_item,
    pattern_id,
    customer_alt_po,
    shipping_country,
    created_at,
    updated_at,
    style_color_key
FROM 
    [dbo].[int_orders_extended];
