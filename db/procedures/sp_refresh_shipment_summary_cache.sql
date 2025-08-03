-- =====================================================================================
-- TASK001: Shipment Summary Cache Refresh Stored Procedure
-- =====================================================================================
-- Purpose: Pre-compute complex shipment summary data for performance optimization
-- Replaces: Complex real-time aggregation query causing 2-5 second response times
-- Performance: Target <1 second UI response with this cached approach
-- Created: August 3, 2025

CREATE OR ALTER PROCEDURE sp_refresh_shipment_summary_cache
    @customer_name NVARCHAR(100) = NULL,           -- NULL = refresh all customers
    @incremental BIT = 0,                          -- 0 = full refresh, 1 = incremental only
    @shipment_ids NVARCHAR(MAX) = NULL,            -- Comma-separated IDs for specific refresh
    @debug BIT = 0                                 -- 1 = verbose output for debugging
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2 = GETDATE();
    DECLARE @rows_processed INT = 0;
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_updated INT = 0;
    DECLARE @error_count INT = 0;
    
    IF @debug = 1
        PRINT 'Starting shipment summary cache refresh at ' + CONVERT(VARCHAR(23), @start_time, 121);
    
    BEGIN TRY
        -- Create temp table for processing
        CREATE TABLE #shipment_summary_temp (
            shipment_id INT,
            customer_name NVARCHAR(100),
            row_number INT,
            style_code NVARCHAR(50),
            color_description NVARCHAR(100),
            delivery_method NVARCHAR(50),
            quantity INT,
            style_match_indicator CHAR(1),
            color_match_indicator CHAR(1),
            delivery_match_indicator CHAR(1),
            quantity_match_indicator CHAR(1),
            match_count INT,
            match_layers NVARCHAR(50),
            best_confidence DECIMAL(5,2),
            avg_confidence DECIMAL(5,2),
            total_matched_order_qty INT,
            quantity_variance INT,
            shipment_status NVARCHAR(20),
            outstanding_reviews INT,
            source_last_modified DATETIME2
        );
        
        -- Build WHERE clause for filtering
        DECLARE @where_clause NVARCHAR(MAX) = 'WHERE 1=1';
        
        IF @customer_name IS NOT NULL
            SET @where_clause = @where_clause + ' AND s.customer_name LIKE ''' + @customer_name + '%''';
            
        IF @shipment_ids IS NOT NULL
            SET @where_clause = @where_clause + ' AND s.shipment_id IN (' + @shipment_ids + ')';
        
        -- Build the main aggregation query (based on debug14 enhanced query)
        DECLARE @sql NVARCHAR(MAX) = '
        INSERT INTO #shipment_summary_temp
        SELECT 
            s.shipment_id,
            s.customer_name,
            
            -- Row number calculation (prioritize problem shipments)
            ROW_NUMBER() OVER (
                PARTITION BY s.customer_name 
                ORDER BY 
                    CASE 
                        WHEN COUNT(CASE WHEN emr.quantity_check_result = ''FAIL'' THEN 1 END) > 0 THEN 1
                        WHEN COUNT(CASE WHEN emr.delivery_match = ''MISMATCH'' THEN 1 END) > 0 THEN 2  
                        ELSE 3
                    END,
                    s.shipment_id
            ) as row_number,
            
            -- Core shipment data
            s.style_code,
            s.color_description,
            s.delivery_method,
            s.quantity,
            
            -- Match indicators (Y/N/P/U logic based on enhanced_matching_results)
            CASE 
                WHEN MAX(CASE WHEN emr.style_match = ''MATCH'' THEN 1 ELSE 0 END) = 1 THEN ''Y''
                WHEN MAX(CASE WHEN emr.style_match = ''FUZZY'' THEN 1 ELSE 0 END) = 1 THEN ''P''
                WHEN COUNT(emr.id) > 0 THEN ''N''
                ELSE ''U''
            END as style_match_indicator,
            
            CASE 
                WHEN MAX(CASE WHEN emr.color_match = ''MATCH'' THEN 1 ELSE 0 END) = 1 THEN ''Y''
                WHEN MAX(CASE WHEN emr.color_match = ''FUZZY'' THEN 1 ELSE 0 END) = 1 THEN ''P''
                WHEN COUNT(emr.id) > 0 THEN ''N''
                ELSE ''U''
            END as color_match_indicator,
            
            CASE 
                WHEN MAX(CASE WHEN emr.delivery_match = ''MISMATCH'' THEN 1 ELSE 0 END) = 1 THEN ''N''
                WHEN MAX(CASE WHEN emr.delivery_match = ''MATCH'' THEN 1 ELSE 0 END) = 1 THEN ''Y''
                WHEN COUNT(emr.id) > 0 THEN ''P''
                ELSE ''U''
            END as delivery_match_indicator,
            
            -- Quantity match indicator
            CASE 
                WHEN s.quantity - ISNULL(SUM(emr.order_quantity), 0) = 0 THEN ''Y''
                WHEN ABS(s.quantity - ISNULL(SUM(emr.order_quantity), 0)) <= s.quantity * 0.1 THEN ''P''
                WHEN COUNT(emr.id) > 0 THEN ''N''
                ELSE ''U''
            END as quantity_match_indicator,
            
            -- Match metadata
            COUNT(emr.id) as match_count,
            
            -- Consolidated layer information (simplified approach)
            CASE 
                WHEN COUNT(emr.id) = 0 THEN NULL
                WHEN MIN(emr.match_layer) = MAX(emr.match_layer) THEN MIN(emr.match_layer)
                ELSE MIN(emr.match_layer) + ''-'' + MAX(emr.match_layer)
            END as match_layers,
            
            -- Confidence levels
            ISNULL(MAX(emr.match_confidence), 0.00) as best_confidence,
            ISNULL(AVG(emr.match_confidence), 0.00) as avg_confidence,
            
            -- Quantity analysis
            ISNULL(SUM(emr.order_quantity), 0) as total_matched_order_qty,
            s.quantity - ISNULL(SUM(emr.order_quantity), 0) as quantity_variance,
            
            -- Overall status classification
            CASE 
                WHEN COUNT(emr.id) = 0 THEN ''UNMATCHED''
                WHEN COUNT(CASE WHEN emr.quantity_check_result = ''FAIL'' THEN 1 END) > 0 THEN ''QUANTITY_ISSUES''
                WHEN COUNT(CASE WHEN emr.delivery_match = ''MISMATCH'' THEN 1 END) > 0 THEN ''DELIVERY_ISSUES''
                ELSE ''GOOD''
            END as shipment_status,
            
            -- Outstanding reviews (placeholder)
            0 as outstanding_reviews,
            
            -- Source timestamp tracking
            GETDATE() as source_last_modified
            
        FROM stg_fm_orders_shipped_table s
        LEFT JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
        ' + @where_clause + '
        GROUP BY s.shipment_id, s.customer_name, s.style_code, s.color_description, s.delivery_method, s.quantity';
        
        IF @debug = 1
            PRINT 'Executing aggregation query...';
            
        -- Execute the main query
        EXEC sp_executesql @sql;
        
        SET @rows_processed = @@ROWCOUNT;
        
        IF @debug = 1
            PRINT 'Processed ' + CAST(@rows_processed AS VARCHAR(10)) + ' shipment records';
        
        -- UPSERT logic: Update existing records, insert new ones
        BEGIN TRANSACTION;
        
        -- Update existing records
        UPDATE ssc
        SET 
            customer_name = temp.customer_name,
            row_number = temp.row_number,
            style_code = temp.style_code,
            color_description = temp.color_description,
            delivery_method = temp.delivery_method,
            quantity = temp.quantity,
            style_match_indicator = temp.style_match_indicator,
            color_match_indicator = temp.color_match_indicator,
            delivery_match_indicator = temp.delivery_match_indicator,
            quantity_match_indicator = temp.quantity_match_indicator,
            match_count = temp.match_count,
            match_layers = temp.match_layers,
            best_confidence = temp.best_confidence,
            avg_confidence = temp.avg_confidence,
            total_matched_order_qty = temp.total_matched_order_qty,
            quantity_variance = temp.quantity_variance,
            shipment_status = temp.shipment_status,
            outstanding_reviews = temp.outstanding_reviews,
            last_updated = GETDATE(),
            source_last_modified = temp.source_last_modified
        FROM dbo.shipment_summary_cache ssc
        INNER JOIN #shipment_summary_temp temp ON ssc.shipment_id = temp.shipment_id;
        
        SET @rows_updated = @@ROWCOUNT;
        
        -- Insert new records
        INSERT INTO dbo.shipment_summary_cache (
            shipment_id, customer_name, row_number, style_code, color_description, delivery_method, quantity,
            style_match_indicator, color_match_indicator, delivery_match_indicator, quantity_match_indicator,
            match_count, match_layers, best_confidence, avg_confidence, total_matched_order_qty, quantity_variance,
            shipment_status, outstanding_reviews, last_updated, source_last_modified
        )
        SELECT 
            temp.shipment_id, temp.customer_name, temp.row_number, temp.style_code, temp.color_description, 
            temp.delivery_method, temp.quantity, temp.style_match_indicator, temp.color_match_indicator, 
            temp.delivery_match_indicator, temp.quantity_match_indicator, temp.match_count, temp.match_layers, 
            temp.best_confidence, temp.avg_confidence, temp.total_matched_order_qty, temp.quantity_variance,
            temp.shipment_status, temp.outstanding_reviews, GETDATE(), temp.source_last_modified
        FROM #shipment_summary_temp temp
        LEFT JOIN dbo.shipment_summary_cache ssc ON temp.shipment_id = ssc.shipment_id
        WHERE ssc.shipment_id IS NULL;
        
        SET @rows_inserted = @@ROWCOUNT;
        
        -- Clean up stale records if doing full refresh
        IF @incremental = 0 AND @customer_name IS NOT NULL
        BEGIN
            DELETE FROM dbo.shipment_summary_cache 
            WHERE customer_name LIKE @customer_name + '%'
              AND shipment_id NOT IN (SELECT shipment_id FROM #shipment_summary_temp);
        END
        
        COMMIT TRANSACTION;
        
        DECLARE @end_time DATETIME2 = GETDATE();
        DECLARE @duration_ms INT = DATEDIFF(MILLISECOND, @start_time, @end_time);
        
        -- Log results
        PRINT 'Cache refresh completed successfully:';
        PRINT '  Duration: ' + CAST(@duration_ms AS VARCHAR(10)) + 'ms';
        PRINT '  Rows processed: ' + CAST(@rows_processed AS VARCHAR(10));
        PRINT '  Rows updated: ' + CAST(@rows_updated AS VARCHAR(10));
        PRINT '  Rows inserted: ' + CAST(@rows_inserted AS VARCHAR(10));
        
        -- Return summary statistics
        SELECT 
            @duration_ms as duration_ms,
            @rows_processed as rows_processed,
            @rows_updated as rows_updated,
            @rows_inserted as rows_inserted,
            @error_count as error_count,
            'SUCCESS' as status;
            
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        SET @error_count = 1;
        
        DECLARE @error_message NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @error_line INT = ERROR_LINE();
        
        PRINT 'Cache refresh failed:';
        PRINT '  Error: ' + @error_message;
        PRINT '  Line: ' + CAST(@error_line AS VARCHAR(10));
        
        -- Return error information
        SELECT 
            0 as duration_ms,
            @rows_processed as rows_processed,
            0 as rows_updated,
            0 as rows_inserted,
            @error_count as error_count,
            'ERROR: ' + @error_message as status;
            
        -- Re-raise the error for calling applications
        THROW;
    END CATCH
    
    -- Cleanup
    DROP TABLE #shipment_summary_temp;
END

GO

-- Create convenience procedure for common refresh patterns
CREATE OR ALTER PROCEDURE sp_refresh_customer_cache
    @customer_name NVARCHAR(100),
    @debug BIT = 0
AS
BEGIN
    EXEC sp_refresh_shipment_summary_cache 
        @customer_name = @customer_name,
        @incremental = 0,
        @debug = @debug;
END

GO

-- Performance testing query template
/*
-- Test cache refresh performance
EXEC sp_refresh_shipment_summary_cache @customer_name = 'GREYSON', @debug = 1;

-- Verify results
SELECT TOP 10 * FROM dbo.shipment_summary_cache WHERE customer_name LIKE 'GREYSON%' ORDER BY row_number;

-- Check cache statistics
SELECT * FROM vw_shipment_summary_cache_stats;
*/

PRINT 'Created sp_refresh_shipment_summary_cache stored procedure';
PRINT 'Ready for UI integration in next step';
