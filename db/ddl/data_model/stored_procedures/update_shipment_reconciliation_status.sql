-- update_shipment_reconciliation_status Stored Procedure
-- Updates reconciliation status for shipments

CREATE OR ALTER PROCEDURE [dbo].[update_shipment_reconciliation_status]
    @reconciliation_id INT,
    @status NVARCHAR(20),
    @user NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @shipment_id INT;
    DECLARE @split_group_id NVARCHAR(100);
    
    -- Get shipment ID and split group from reconciliation result
    SELECT 
        @shipment_id = shipment_id,
        @split_group_id = split_group_id
    FROM 
        [dbo].[reconciliation_result]
    WHERE 
        id = @reconciliation_id;
    
    -- Update the shipment record
    UPDATE [dbo].[FM_orders_shipped]
    SET 
        reconciliation_status = @status,
        reconciliation_id = @reconciliation_id,
        reconciliation_date = GETDATE(),
        last_reviewed_by = @user,
        last_reviewed_date = CASE WHEN @user IS NOT NULL THEN GETDATE() ELSE last_reviewed_date END
    WHERE 
        id = @shipment_id;
    
    -- If this is part of a split shipment, update all related records
    IF @split_group_id IS NOT NULL
    BEGIN
        UPDATE [dbo].[FM_orders_shipped]
        SET 
            reconciliation_status = @status,
            reconciliation_date = GETDATE(),
            last_reviewed_by = @user,
            last_reviewed_date = CASE WHEN @user IS NOT NULL THEN GETDATE() ELSE last_reviewed_date END
        WHERE 
            split_group_id = @split_group_id AND
            id <> @shipment_id;
    END
    
    -- Add audit log entry
    INSERT INTO [dbo].[reconciliation_audit_log]
    (
        entity_type,
        entity_id,
        action,
        field_name,
        new_value,
        user_id,
        timestamp
    )
    VALUES
    (
        'FM_orders_shipped',
        @shipment_id,
        'update',
        'reconciliation_status',
        @status,
        @user,
        GETDATE()
    );
    
    RETURN 0;
END;
