-- reconciliation_summary_view View
-- Provides a consolidated view of reconciliation results

CREATE VIEW [dbo].[reconciliation_summary_view] AS
SELECT 
    r.[id] AS reconciliation_id,
    r.[customer_name],
    r.[po_number],
    r.[match_status],
    r.[confidence_score],
    r.[match_method],
    r.[is_split_shipment],
    r.[reconciliation_date],
    o.[order_date],
    o.[style],
    o.[color],
    o.[size],
    o.[quantity] AS order_quantity,
    s.[ship_date],
    s.[style_name] AS shipment_style,
    s.[color_name] AS shipment_color,
    s.[size_name] AS shipment_size,
    s.[quantity] AS shipment_quantity,
    h.[status] AS review_status,
    h.[review_decision],
    h.[assigned_to] AS reviewer
FROM 
    [dbo].[reconciliation_result] r
LEFT JOIN 
    [dbo].[ORDERS_UNIFIED] o ON r.[order_id] = o.[id]
LEFT JOIN 
    [dbo].[FM_orders_shipped] s ON r.[shipment_id] = s.[id]
LEFT JOIN 
    [dbo].[hitl_queue] h ON r.[id] = h.[reconciliation_id]
WHERE 
    r.[match_status] IN ('unmatched', 'uncertain');
