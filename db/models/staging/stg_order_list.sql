-- stg_order_list.sql
-- Staging model for FACT_ORDER_LIST with standardized field names
-- This is a 1:1 mapping from the source table with consistent naming
-- Updated to use canonical customer names for consistent matching

CREATE OR ALTER VIEW [dbo].[stg_order_list] AS
SELECT
    ol.[record_uuid] AS order_id,
    cc.canonical AS customer_name,
    ol.[PO NUMBER] AS po_number,
    ol.[PLANNED DELIVERY METHOD] AS delivery_method,
    ol.[CUSTOMER STYLE] AS style_code,
    ol.[CUSTOMER COLOUR DESCRIPTION] AS color_description,
    ol.[CUSTOMER'S COLOUR CODE (CUSTOM FIELD) CUSTOMER PROVIDES THIS] AS customer_color_code,
    ol.[AAG ORDER NUMBER] AS aag_order_number,
    ol.[ORDER TYPE] AS order_type,
    ol.[ORDER DATE PO RECEIVED] AS order_date,
    ol.[TOTAL QTY] AS quantity,
    -- [SIZE] column removed as requested
    -- created_at and updated_at fields are retained for:
    -- 1. Tracking when records were added/modified in the source system
    -- 2. Enabling incremental loads based on these timestamps
    -- 3. Supporting audit trail requirements
    ol.[created_at],
    ol.[updated_at]
FROM 
    [dbo].[ORDER_LIST] ol
join
    [dbo].[canonical_customer_map] cc ON ol.[CUSTOMER NAME] LIKE cc.name

