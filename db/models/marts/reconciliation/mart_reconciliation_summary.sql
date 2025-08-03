-- mart_reconciliation_summary.sql
-- Mart model for reconciliation reporting
-- Joins orders and shipments based on reconciliation results

CREATE OR ALTER VIEW [dbo].[mart_reconciliation_summary] AS
SELECT
    o.order_id,
    s.shipment_id,
    o.customer_name,
    o.po_number,
    o.delivery_method,
    o.style_code,
    o.color_description,
    o.customer_color_code,
    s.size AS shipment_size, -- Only including size from the shipment
    o.quantity AS order_quantity,
    s.quantity AS shipped_quantity,
    o.order_date,
    s.shipped_date,
    o.aag_order_number,
    o.order_type,
    COALESCE(o.alias_related_item, s.alias_related_item) AS alias_related_item,
    COALESCE(o.pattern_id, s.pattern_id) AS pattern_id,
    COALESCE(o.customer_alt_po, s.customer_alt_po) AS customer_alt_po,
    s.reconciliation_status,
    s.reconciliation_id,
    s.reconciliation_date,
    s.split_shipment,
    s.split_group_id,
    s.parent_shipment_id,
    s.last_reviewed_by,
    s.last_reviewed_date,
    
    -- Calculate metrics
    CASE 
        WHEN s.quantity = o.quantity THEN 'FULL'
        WHEN s.quantity < o.quantity THEN 'PARTIAL'
        WHEN s.quantity > o.quantity THEN 'OVERAGE'
    END AS fulfillment_status,
    
    DATEDIFF(day, o.order_date, s.shipped_date) AS days_to_ship
FROM 
    [dbo].[mart_fact_order_list] o
JOIN
    [dbo].[mart_fact_orders_shipped] s ON o.order_id = s.order_reference_id
WHERE
    s.reconciliation_status = 'MATCHED';
