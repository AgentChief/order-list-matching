-- Update_Canonical_Fields Stored Procedure
-- Updates fields in FACT_Orders_Shipped with values from canonical_customers.yaml config

CREATE OR ALTER PROCEDURE [dbo].[Update_Canonical_Fields]
    @customer_name NVARCHAR(255) = NULL -- Optional parameter to update only a specific customer
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @customer_query NVARCHAR(MAX);
    
    -- Set the customer filter based on parameter
    IF @customer_name IS NULL
        SET @customer_query = '';
    ELSE
        SET @customer_query = ' AND cac.customer_name = @customer_name';
    
    -- Update alias_related_item from map_attribute table where available
    UPDATE fs
    SET
        fs.[alias_related_item] = ma.[canonical_value],
        fs.[updated_at] = GETDATE()
    FROM
        [dbo].[FACT_Orders_Shipped] fs
    JOIN
        [dbo].[map_attribute] ma ON 
            ma.[source_value] = fs.[style_code] AND
            ma.[attribute_name] = 'ALIAS/RELATED ITEM' AND
            ma.[source_system] = 'SHIPMENTS' AND
            ma.[is_active] = 1
    JOIN
        [dbo].[customer_attribute_config] cac ON 
            cac.[customer_name] = fs.[customer_name] AND
            cac.[is_active] = 1
    WHERE
        fs.[alias_related_item] IS NULL OR
        fs.[alias_related_item] <> ma.[canonical_value];
    
    -- Update pattern_id from style/color patterns if available
    -- This is a placeholder - actual implementation would depend on pattern logic
    UPDATE fs
    SET
        fs.[pattern_id] = pc.[pattern_id],
        fs.[updated_at] = GETDATE()
    FROM
        [dbo].[FACT_Orders_Shipped] fs
    JOIN
        [dbo].[pattern_catalog] pc ON 
            pc.[style_code] = fs.[style_code] AND
            pc.[color_code] LIKE '%' + fs.[color_description] + '%' AND
            pc.[is_active] = 1
    WHERE
        fs.[pattern_id] IS NULL OR
        fs.[pattern_id] <> pc.[pattern_id];
        
    -- Update customer_alt_po from order data where available
    UPDATE fs
    SET
        fs.[customer_alt_po] = ol.[customer_alt_po],
        fs.[updated_at] = GETDATE()
    FROM
        [dbo].[FACT_Orders_Shipped] fs
    JOIN
        [dbo].[reconciliation_result] rr ON 
            rr.[shipment_id] = fs.[source_shipment_id] AND
            rr.[match_status] = 'matched'
    JOIN
        [dbo].[FACT_ORDER_LIST] ol ON 
            ol.[id] = rr.[order_id]
    LEFT JOIN
        [dbo].[V_FACT_ORDER_LIST_Canonical] olc ON 
            olc.[order_id] = ol.[id] AND
            olc.[customer_alt_po] IS NOT NULL
    WHERE
        (fs.[customer_alt_po] IS NULL OR
         fs.[customer_alt_po] <> COALESCE(olc.[customer_alt_po], ol.[CUSTOMER ALT PO])) AND
        (olc.[customer_alt_po] IS NOT NULL OR ol.[CUSTOMER ALT PO] IS NOT NULL);
    
    -- Return count of affected records
    SELECT
        @@ROWCOUNT AS [RecordsAffected];
    
    RETURN 0;
END;
