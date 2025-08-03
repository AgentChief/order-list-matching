-- V_Order_Shipment_Reconciliation View
-- Joins FACT_ORDER_LIST and FACT_Orders_Shipped using canonical field names
-- for easy querying of reconciled records

CREATE OR ALTER VIEW [dbo].[V_Order_Shipment_Reconciliation] AS
SELECT
    o.order_id,
    s.shipment_id,
    o.customer_name,
    o.po_number,
    o.delivery_method,
    o.style_code,
    o.color_description,
    o.size,
    o.quantity AS order_quantity,
    s.quantity AS shipped_quantity,
    o.order_date,
    s.shipped_date,
    o.aag_order_number,
    o.order_type,
    COALESCE(o.alias_related_item, s.alias_related_item) AS alias_related_item,
    COALESCE(o.original_alias_related_item, s.original_alias_related_item) AS original_alias_related_item,
    COALESCE(o.pattern_id, s.pattern_id) AS pattern_id,
    COALESCE(o.customer_alt_po, s.customer_alt_po) AS customer_alt_po,
    s.shipping_country,
    s.reconciliation_status,
    s.reconciliation_id,
    s.reconciliation_date,
    s.split_shipment,
    s.split_group_id,
    s.parent_shipment_id,
    s.last_reviewed_by,
    s.last_reviewed_date
FROM
    [dbo].[V_FACT_ORDER_LIST_Canonical] o
JOIN
    [dbo].[FACT_Orders_Shipped] s ON o.order_id = s.order_reference_id
WHERE
    s.reconciliation_status = 'MATCHED';
