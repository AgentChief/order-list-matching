-- 03_create_refresh_shipments_procedure.sql
-- Creates a stored procedure to refresh the staging table
-- Updated to handle missing columns in the source table

CREATE OR ALTER PROCEDURE [dbo].[sp_refresh_stg_fm_orders_shipped]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Save current match results temporarily to preserve them
        CREATE TABLE #temp_matches (
            [shipment_id] INT,
            [matched_order_id] NVARCHAR(100),
            [match_confidence] DECIMAL(5,2),
            [match_method] NVARCHAR(50),
            [match_notes] NVARCHAR(MAX),
            [reconciliation_status] NVARCHAR(20),
            [reconciliation_id] INT,
            [reconciliation_date] DATETIME
        );
        
        -- Store current match information and reconciliation status
        INSERT INTO #temp_matches
        SELECT 
            [shipment_id],
            [matched_order_id],
            [match_confidence],
            [match_method],
            [match_notes],
            [reconciliation_status],
            [reconciliation_id],
            [reconciliation_date]
        FROM 
            [dbo].[stg_fm_orders_shipped_table]
        WHERE 
            [matched_order_id] IS NOT NULL OR [reconciliation_status] IS NOT NULL;
            
        -- Clear existing data
        TRUNCATE TABLE [dbo].[stg_fm_orders_shipped_table];
        
        -- Create a temporary table with row numbers
        SELECT 
            ROW_NUMBER() OVER (ORDER BY [Customer], [Customer_PO], [Style], [Color]) AS temp_id,
            [Customer],
            [Customer_PO],
            [Style],
            [Color],
            [Shipping_Method],
            [Shipped_Date],
            [Size],
            [Qty]
        INTO #temp_shipments
        FROM 
            [dbo].[FM_orders_shipped];
        
        -- Now aggregate from the temp table which has a guaranteed ID column
        INSERT INTO [dbo].[stg_fm_orders_shipped_table] (
            [source_shipment_id],
            [customer_name],
            [po_number],
            [style_code],
            [color_description],
            [delivery_method],
            [shipped_date],
            [quantity],
            [size_breakdown],
            [reconciliation_status],
            [created_at],
            [updated_at],
            [last_sync_date]
        )
        SELECT
            MIN(temp_id) AS source_shipment_id,
            [Customer] AS customer_name,
            [Customer_PO] AS po_number,
            [Style] AS style_code,
            [Color] AS color_description,
            [Shipping_Method] AS delivery_method,
            [Shipped_Date] AS shipped_date,
            SUM([Qty]) AS quantity,
            STRING_AGG([Size] + '(' + CAST([Qty] AS VARCHAR) + ')', ', ') AS size_breakdown,
            'UNMATCHED' AS reconciliation_status, -- Default status
            GETDATE() AS created_at,
            GETDATE() AS updated_at,
            GETDATE() AS last_sync_date
        FROM 
            #temp_shipments
        GROUP BY
            [Customer],
            [Customer_PO],
            [Style],
            [Color],
            [Shipping_Method],
            [Shipped_Date];

        -- Clean up the temp table
        DROP TABLE #temp_shipments;
            
        -- Restore the match information
        UPDATE s
        SET 
            s.[matched_order_id] = t.[matched_order_id],
            s.[match_confidence] = t.[match_confidence],
            s.[match_method] = t.[match_method],
            s.[match_notes] = t.[match_notes],
            s.[reconciliation_status] = t.[reconciliation_status],
            s.[reconciliation_id] = t.[reconciliation_id],
            s.[reconciliation_date] = t.[reconciliation_date]
        FROM 
            [dbo].[stg_fm_orders_shipped_table] s
        INNER JOIN 
            #temp_matches t ON s.[shipment_id] = t.[shipment_id];
            
        -- Drop the temporary table
        DROP TABLE #temp_matches;
        
        -- Update reconciliation status for matched records if needed
        UPDATE [dbo].[stg_fm_orders_shipped_table]
        SET [reconciliation_status] = 'MATCHED'
        WHERE [matched_order_id] IS NOT NULL
        AND ([reconciliation_status] IS NULL OR [reconciliation_status] <> 'MATCHED');
        
        -- Get count values for reporting
        DECLARE @total_count INT;
        DECLARE @matched_count INT;
        
        SELECT @total_count = COUNT(*) FROM [dbo].[stg_fm_orders_shipped_table];
        SELECT @matched_count = COUNT(*) FROM [dbo].[stg_fm_orders_shipped_table] WHERE [matched_order_id] IS NOT NULL;
        
        COMMIT TRANSACTION;
        
        PRINT 'Successfully refreshed stg_fm_orders_shipped_table.';
        PRINT 'Total records: ' + CAST(@total_count AS VARCHAR);
        PRINT 'Matched records: ' + CAST(@matched_count AS VARCHAR);
        
        RETURN 0; -- Success
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
        
        RETURN -1; -- Error
    END CATCH;
END;
