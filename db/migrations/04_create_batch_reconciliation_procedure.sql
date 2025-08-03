-- 04_create_batch_reconciliation_procedure.sql
-- Creates a stored procedure to run batch reconciliation

CREATE OR ALTER PROCEDURE [dbo].[sp_batch_reconcile_shipments]
    @customer_name NVARCHAR(255) = NULL,
    @po_number NVARCHAR(100) = NULL,
    @fuzzy_threshold INT = 85,
    @batch_description NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Create a reconciliation batch record
        DECLARE @batch_id INT;
        DECLARE @batch_name NVARCHAR(255) = COALESCE(@batch_description, 
            'Reconciliation ' + CONVERT(VARCHAR, GETDATE(), 120) + 
            CASE 
                WHEN @customer_name IS NOT NULL THEN ' - ' + @customer_name 
                ELSE '' 
            END +
            CASE 
                WHEN @po_number IS NOT NULL THEN ' - PO ' + @po_number 
                ELSE '' 
            END);
        
        INSERT INTO [dbo].[reconciliation_batch] (
            [name],
            [description],
            [start_time],
            [fuzzy_threshold],
            [created_at]
        )
        VALUES (
            @batch_name,
            @batch_description,
            GETDATE(),
            @fuzzy_threshold,
            GETDATE()
        );
        
        SET @batch_id = SCOPE_IDENTITY();
        
        -- Create a temp table to store matches
        CREATE TABLE #matches (
            [shipment_id] INT,
            [order_id] NVARCHAR(100),
            [match_confidence] DECIMAL(5,2),
            [match_method] NVARCHAR(50),
            [match_notes] NVARCHAR(MAX)
        );
        
        -- Get shipments to reconcile
        DECLARE @shipment_count INT = 0;
        
        -- Exact matching by style_color_key and customer_po_key
        INSERT INTO #matches (shipment_id, order_id, match_confidence, match_method, match_notes)
        SELECT 
            s.[shipment_id],
            o.[order_id],
            100.0 AS match_confidence,
            'EXACT' AS match_method,
            'Exact match by style-color and customer-PO' AS match_notes
        FROM 
            [dbo].[stg_fm_orders_shipped_table] s
        INNER JOIN 
            [dbo].[int_orders_extended] o ON s.[style_color_key] = o.[style_color_key]
                                            AND s.[customer_po_key] = CONCAT(o.[customer_name], '-', o.[po_number])
        WHERE
            (@customer_name IS NULL OR s.[customer_name] = @customer_name)
            AND (@po_number IS NULL OR s.[po_number] = @po_number)
            AND s.[matched_order_id] IS NULL;
        
        -- Count how many shipments we found
        SET @shipment_count = @@ROWCOUNT;
        
        -- Here you would add your fuzzy matching logic if exact matching didn't find all records
        -- This would involve a similarity algorithm on style_code, color_description, etc.
        
        -- Update the shipment records with match information
        UPDATE s
        SET 
            s.[matched_order_id] = m.[order_id],
            s.[match_confidence] = m.[match_confidence],
            s.[match_method] = m.[match_method],
            s.[match_notes] = m.[match_notes],
            s.[reconciliation_status] = 'MATCHED',
            s.[reconciliation_id] = @batch_id,
            s.[reconciliation_date] = GETDATE(),
            s.[updated_at] = GETDATE()
        FROM 
            [dbo].[stg_fm_orders_shipped_table] s
        INNER JOIN 
            #matches m ON s.[shipment_id] = m.[shipment_id];
        
        -- Update the batch record with completion information
        UPDATE [dbo].[reconciliation_batch]
        SET 
            [end_time] = GETDATE(),
            [matched_count] = @shipment_count,
            [updated_at] = GETDATE(),
            [status] = 'COMPLETED'
        WHERE 
            [id] = @batch_id;
        
        -- Drop the temp table
        DROP TABLE #matches;
        
        -- Return the batch ID
        SELECT @batch_id AS batch_id, @shipment_count AS matched_count;
        
        COMMIT TRANSACTION;
        
        RETURN 0; -- Success
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        -- Update the batch record with error information
        IF @batch_id IS NOT NULL
        BEGIN
            UPDATE [dbo].[reconciliation_batch]
            SET 
                [end_time] = GETDATE(),
                [status] = 'ERROR',
                [error_message] = @ErrorMessage,
                [updated_at] = GETDATE()
            WHERE 
                [id] = @batch_id;
        END
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
        
        RETURN -1; -- Error
    END CATCH;
END;
