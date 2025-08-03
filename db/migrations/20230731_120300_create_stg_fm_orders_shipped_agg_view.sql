-- create_stg_fm_orders_shipped_agg_view.sql
-- Create a view for aggregated shipment data that references the staging table

CREATE OR ALTER VIEW [dbo].[stg_fm_orders_shipped_agg] AS
SELECT
    [representative_shipment_id] AS shipment_id,
    [customer_name],
    [po_number],
    [style_code],
    [color_description],
    [delivery_method],
    [shipped_date],
    [quantity],
    [size_breakdown],
    [reconciliation_status],
    [reconciliation_id],
    [reconciliation_date],
    [split_shipment],
    [split_group_id],
    [parent_shipment_id],
    [last_reviewed_by],
    [last_reviewed_date],
    [matched_order_id],
    [match_confidence],
    [match_method],
    [match_date],
    [created_at],
    [updated_at],
    [last_sync_date]
FROM 
    [dbo].[stg_fm_orders_shipped_table];
