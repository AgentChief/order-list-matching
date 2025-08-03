-- V_FACT_ORDER_LIST_Canonical View
-- Provides canonical field names for FACT_ORDER_LIST to align with FACT_Orders_Shipped

CREATE OR ALTER VIEW [dbo].[V_FACT_ORDER_LIST_Canonical] AS
SELECT
    id AS order_id,
    [CUSTOMER NAME] AS customer_name,
    [PO NUMBER] AS po_number,
    [PLANNED DELIVERY METHOD] AS delivery_method,
    [CUSTOMER STYLE] AS style_code,
    [CUSTOMER COLOUR DESCRIPTION] AS color_description,
    [AAG ORDER NUMBER] AS aag_order_number,
    [ORDER TYPE] AS order_type,
    [ORDER DATE PO RECEIVED] AS order_date,
    [QUANTITY] AS quantity,
    [SIZE] AS size,
    -- Add fields from canonical mapping that aren't in the base table
    -- These will be NULL unless populated via additional logic
    NULL AS alias_related_item,
    NULL AS original_alias_related_item,
    NULL AS pattern_id,
    NULL AS customer_alt_po,
    NULL AS shipping_country,
    created_at,
    updated_at
FROM 
    [dbo].[FACT_ORDER_LIST];
