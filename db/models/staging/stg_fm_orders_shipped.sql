-- stg_fm_orders_shipped.sql
-- Staging model for FM_orders_shipped with standardized field names
-- This view now references the staging table instead of the source table

CREATE OR ALTER VIEW [dbo].[stg_fm_orders_shipped] AS
SELECT
    [shipment_id],
    [customer_name],
    [po_number],
    [style_code],
    [color_description],
    [delivery_method],
    [shipped_date],
    [quantity],
    [size_breakdown],
    [style_color_key],
    [customer_po_key],
    [reconciliation_status],
    [reconciliation_id],
    [reconciliation_date],
    [split_shipment],
    [split_group_id],
    [parent_shipment_id],
    [matched_order_id],
    [match_confidence],
    [match_method],
    [match_notes],
    [last_reviewed_by],
    [last_reviewed_date],
    [created_at],
    [updated_at],
    [last_sync_date]
FROM 
    [dbo].[stg_fm_orders_shipped_table];



