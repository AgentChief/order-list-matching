-- int_orders_extended.sql
-- Intermediate model that extends orders with additional attributes from canonical config
-- Adds fields required for matching but not in the original table

CREATE OR ALTER VIEW [dbo].[int_orders_extended] AS
SELECT
    o.*,
    
    -- The following fields would ideally come from a proper mapping table
    -- Currently they are NULL placeholders for the join with shipments
    NULL AS alias_related_item,
    NULL AS original_alias_related_item,
    NULL AS pattern_id,
    NULL AS customer_alt_po,
    NULL AS shipping_country,
    
    -- Add any derived fields here
    -- For example, concatenating style and color for pattern matching
    CONCAT(o.style_code, '-', o.color_description) AS style_color_key
FROM 
    [dbo].[stg_order_list] o;
