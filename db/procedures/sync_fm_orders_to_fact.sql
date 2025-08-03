-- sync_fm_orders_to_fact.sql
-- Procedure to synchronize data from FM_orders_shipped to FACT_Orders_Shipped

CREATE OR ALTER PROCEDURE [dbo].[Sync_FM_Orders_To_FACT]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert new records from FM_orders_shipped that don't exist in FACT_Orders_Shipped
    INSERT INTO [dbo].[FACT_Orders_Shipped]
    (
        [source_shipment_id],
        [customer_name],
        [po_number],
        [style_code],
        [color_description],
        [delivery_method],
        [shipped_date],
        [quantity],
        [size],
        [reconciliation_status],
        [reconciliation_id],
        [reconciliation_date],
        [split_shipment],
        [split_group_id],
        [parent_shipment_id],
        [last_reviewed_by],
        [last_reviewed_date],
        [created_at],
        [updated_at]
    )
    SELECT
        fm.[id],
        fm.[Customer],
        fm.[Customer_PO],
        fm.[Style],
        fm.[Color],
        fm.[Shipping_Method],
        fm.[Shipped_Date],
        fm.[Qty],
        fm.[Size],
        fm.[reconciliation_status],
        fm.[reconciliation_id],
        fm.[reconciliation_date],
        fm.[split_shipment],
        fm.[split_group_id],
        fm.[parent_shipment_id],
        fm.[last_reviewed_by],
        fm.[last_reviewed_date],
        fm.[created_at],
        fm.[updated_at]
    FROM
        [dbo].[FM_orders_shipped] fm
    LEFT JOIN
        [dbo].[FACT_Orders_Shipped] fs ON fm.[id] = fs.[source_shipment_id]
    WHERE
        fs.[source_shipment_id] IS NULL;
    
    -- Update existing records with any changes from FM_orders_shipped
    UPDATE fs
    SET
        fs.[customer_name] = fm.[Customer],
        fs.[po_number] = fm.[Customer_PO],
        fs.[style_code] = fm.[Style],
        fs.[color_description] = fm.[Color],
        fs.[delivery_method] = fm.[Shipping_Method],
        fs.[shipped_date] = fm.[Shipped_Date],
        fs.[quantity] = fm.[Qty],
        fs.[size] = fm.[Size],
        fs.[reconciliation_status] = fm.[reconciliation_status],
        fs.[reconciliation_id] = fm.[reconciliation_id],
        fs.[reconciliation_date] = fm.[reconciliation_date],
        fs.[split_shipment] = fm.[split_shipment],
        fs.[split_group_id] = fm.[split_group_id],
        fs.[parent_shipment_id] = fm.[parent_shipment_id],
        fs.[last_reviewed_by] = fm.[last_reviewed_by],
        fs.[last_reviewed_date] = fm.[last_reviewed_date],
        fs.[updated_at] = GETDATE()
    FROM
        [dbo].[FACT_Orders_Shipped] fs
    JOIN
        [dbo].[FM_orders_shipped] fm ON fs.[source_shipment_id] = fm.[id]
    WHERE
        fs.[customer_name] <> fm.[Customer] OR
        fs.[po_number] <> fm.[Customer_PO] OR
        fs.[style_code] <> fm.[Style] OR
        fs.[color_description] <> fm.[Color] OR
        fs.[delivery_method] <> fm.[Shipping_Method] OR
        fs.[shipped_date] <> fm.[Shipped_Date] OR
        fs.[quantity] <> fm.[Qty] OR
        fs.[size] <> fm.[Size] OR
        (fs.[reconciliation_status] <> fm.[reconciliation_status] OR (fs.[reconciliation_status] IS NULL AND fm.[reconciliation_status] IS NOT NULL) OR (fs.[reconciliation_status] IS NOT NULL AND fm.[reconciliation_status] IS NULL)) OR
        (fs.[reconciliation_id] <> fm.[reconciliation_id] OR (fs.[reconciliation_id] IS NULL AND fm.[reconciliation_id] IS NOT NULL) OR (fs.[reconciliation_id] IS NOT NULL AND fm.[reconciliation_id] IS NULL)) OR
        (fs.[reconciliation_date] <> fm.[reconciliation_date] OR (fs.[reconciliation_date] IS NULL AND fm.[reconciliation_date] IS NOT NULL) OR (fs.[reconciliation_date] IS NOT NULL AND fm.[reconciliation_date] IS NULL)) OR
        (fs.[split_shipment] <> fm.[split_shipment] OR (fs.[split_shipment] IS NULL AND fm.[split_shipment] IS NOT NULL) OR (fs.[split_shipment] IS NOT NULL AND fm.[split_shipment] IS NULL)) OR
        (fs.[split_group_id] <> fm.[split_group_id] OR (fs.[split_group_id] IS NULL AND fm.[split_group_id] IS NOT NULL) OR (fs.[split_group_id] IS NOT NULL AND fm.[split_group_id] IS NULL)) OR
        (fs.[parent_shipment_id] <> fm.[parent_shipment_id] OR (fs.[parent_shipment_id] IS NULL AND fm.[parent_shipment_id] IS NOT NULL) OR (fs.[parent_shipment_id] IS NOT NULL AND fm.[parent_shipment_id] IS NULL)) OR
        (fs.[last_reviewed_by] <> fm.[last_reviewed_by] OR (fs.[last_reviewed_by] IS NULL AND fm.[last_reviewed_by] IS NOT NULL) OR (fs.[last_reviewed_by] IS NOT NULL AND fm.[last_reviewed_by] IS NULL)) OR
        (fs.[last_reviewed_date] <> fm.[last_reviewed_date] OR (fs.[last_reviewed_date] IS NULL AND fm.[last_reviewed_date] IS NOT NULL) OR (fs.[last_reviewed_date] IS NOT NULL AND fm.[last_reviewed_date] IS NULL));
    
    -- Update the order_reference_id based on reconciliation results
    UPDATE fs
    SET
        fs.[order_reference_id] = rr.[order_id],
        fs.[updated_at] = GETDATE()
    FROM
        [dbo].[FACT_Orders_Shipped] fs
    JOIN
        [dbo].[reconciliation_result] rr ON fs.[reconciliation_id] = rr.[id]
    WHERE
        rr.[match_status] = 'matched' AND
        (fs.[order_reference_id] <> rr.[order_id] OR fs.[order_reference_id] IS NULL);
    
    -- Return count of affected records
    SELECT
        @@ROWCOUNT AS [RecordsAffected];
    
    RETURN 0;
END;
