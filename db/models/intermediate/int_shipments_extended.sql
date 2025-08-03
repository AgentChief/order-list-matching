-- int_shipments_extended.sql
-- Intermediate model that extends shipments with additional attributes
-- Adds calculated fields and prepares for joining with orders


CREATE OR ALTER VIEW [dbo].[int_shipments_extended] AS
SELECT
    s.*,
    
    -- The following fields would ideally come from a map_attribute table
    -- but are NULL placeholders for now
    NULL AS alias_related_item,
    NULL AS original_alias_related_item,
    NULL AS pattern_id,
    NULL AS customer_alt_po,
    NULL AS shipping_country,
    
    -- We're now getting style_color_key directly from the table
    -- No need to recalculate it here
    
    -- Reference to the matched order, if available
    r.order_id AS order_reference_id
FROM 
    [dbo].[stg_fm_orders_shipped] s
LEFT JOIN
    [dbo].[reconciliation_result] r ON s.reconciliation_id = r.id AND r.match_status = 'matched';
